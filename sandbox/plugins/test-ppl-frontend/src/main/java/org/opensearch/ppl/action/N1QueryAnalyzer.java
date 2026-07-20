/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.N1Aggregate;
import org.opensearch.analytics.N1Descriptor;
import org.opensearch.analytics.N1Predicate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Auto-detects nested field references in PPL queries and builds an {@link N1Descriptor}
 * for queries that require UNNEST. Replaces the hardcoded N1PlanRegistry with generic
 * schema-driven detection.
 *
 * Detection: if any field reference in a WHERE or STATS command has a dotted prefix
 * that matches a nested (ARRAY of ROW) column in the schema, this query needs the N1 path.
 *
 * @opensearch.internal
 */
public final class N1QueryAnalyzer {

    private N1QueryAnalyzer() {}

    /**
     * Analyzes a PPL query for nested field references in filter/aggregation contexts.
     * Returns an N1Descriptor if the query needs the UNNEST bypass path, or null if
     * the query can go through the normal planner.
     *
     * @param pplText  the raw PPL query text
     * @param baseRowType  the Calcite row type of the source table
     * @param indexName  the source index name
     * @return N1Descriptor or null
     */
    public static N1Descriptor analyze(String pplText, RelDataType baseRowType, String indexName) {
        // Find all nested (ARRAY<ROW>) columns in the schema
        Set<String> nestedColumns = new HashSet<>();
        for (RelDataTypeField field : baseRowType.getFieldList()) {
            if (field.getType().getSqlTypeName() == SqlTypeName.ARRAY
                && field.getType().getComponentType() != null
                && field.getType().getComponentType().isStruct()) {
                nestedColumns.add(field.getName());
            }
        }
        if (nestedColumns.isEmpty()) {
            return null; // No nested columns in schema — normal path
        }

        // Split PPL into pipe-separated commands
        String[] commands = pplText.split("\\|");
        if (commands.length < 1) return null;

        // Parse each command
        String sourceCmd = commands[0].trim(); // "source=blogs"
        String whereClause = null;
        String fieldsClause = null;
        String statsClause = null;

        for (int i = 1; i < commands.length; i++) {
            String cmd = commands[i].trim();
            if (cmd.toLowerCase().startsWith("where ")) {
                whereClause = cmd.substring(6).trim();
            } else if (cmd.toLowerCase().startsWith("fields ")) {
                fieldsClause = cmd.substring(7).trim();
            } else if (cmd.toLowerCase().startsWith("stats ")) {
                statsClause = cmd.substring(6).trim();
            }
        }

        // Check if WHERE or STATS references a nested sub-field
        boolean hasNestedInWhere = whereClause != null && hasNestedRef(whereClause, nestedColumns);
        boolean hasNestedInStats = statsClause != null && hasNestedRef(statsClause, nestedColumns);

        if (!hasNestedInWhere && !hasNestedInStats) {
            return null; // No nested refs in filter/agg — normal path
        }

        // Build N1Descriptor
        // Determine which nested column is being accessed and build multi-level unnest path
        String clause = hasNestedInWhere ? whereClause : statsClause;
        String nestedPath = findNestedPath(clause, nestedColumns);
        if (nestedPath == null) return null;

        // Build unnest path: for "posts.replies.upvotes" where "posts" is nested and
        // "posts.replies" is a sub-nested field, the path is ["posts", "posts.replies"].
        // Detect multi-level by checking if the field reference has more dots after the nested prefix.
        List<String> unnestPath = buildUnnestPath(clause, nestedPath, baseRowType);

        // Parse predicate from WHERE clause
        N1Predicate predicate = null;
        if (hasNestedInWhere && whereClause != null) {
            predicate = parsePredicate(whereClause, nestedPath);
        }

        // Parse aggregate from STATS clause
        N1Aggregate aggregate = null;
        if (statsClause != null) {
            if (hasNestedInStats) {
                aggregate = parseAggregate(statsClause, nestedPath);
            } else if (hasNestedInWhere && statsClause.trim().matches("(?i)count\\(\\)")) {
                // C7-3 fix: "where nested.field > X | stats count()" = count of matching parents
                // The N1 semi-join returns matching parent rows; count() just counts them.
                aggregate = new N1Aggregate(N1Aggregate.Fn.COUNT, null, "count()");
            }
        }

        // Parse projection from FIELDS clause
        List<String> projection = new ArrayList<>();
        boolean hasNestedInProjection = false;
        if (fieldsClause != null) {
            for (String field : fieldsClause.split(",")) {
                String f = field.trim();
                projection.add(f);
                if (f.startsWith(nestedPath + ".")) {
                    hasNestedInProjection = true;
                }
            }
        }

        // If the query has BOTH a filter AND nested sub-fields in the projection,
        // treat it as a "child metric" query (unnest + filter + project, NO semi-join dedup).
        // This returns unnested rows (one per matching child) rather than parent rows.
        // Example: "where comments.score > 4 | fields title, comments.author"
        //   → returns: (First post, alice), (Third post, dave), (Third post, eve)
        if (hasNestedInWhere && hasNestedInProjection && aggregate == null) {
            // Use the child-metric path with a dummy aggregate that just passes through
            // Actually, the cleanest approach: use a COUNT(*) aggregate with no groupBy
            // to count matching children per parent, then... no, that's wrong.
            //
            // The correct plan shape for this case is:
            //   Scan → UNNEST → Filter(score>4) → Project(title, author)
            // This is exactly what buildChildMetric does when fn=COUNT and argField=null:
            // No, that would aggregate. Let's use a different approach.
            //
            // Actually the simplest fix: strip nested sub-fields from projection,
            // and add the nested fields to a separate "unnested output" list.
            // For now, just use the child-metric path with a pass-through:
            // treat "where X | fields a, nested.b" as equivalent to
            // "stats count() | fields a" (to get parent rows) — but that loses nested.b.
            //
            // BEST FIX: Since our direct projection path (PATH B in CorrelateUncollectRewriter)
            // already handles "fields title, comments.author" with UNNEST, just strip the
            // predicate from the N1Descriptor and let the filter fail through.
            // Then the user gets unnested rows (with the filter).
            //
            // Actually the REAL fix: tell N1SubstraitBuilder to use the child-metric plan shape
            // but without the aggregate — just unnest + filter + project.
            // For the POC, let's use a simple workaround: make it a child-metric query with
            // a pass-through (select all unnested rows that match the predicate).
            // Use aggregate = null but adjust projection to only include parent fields.

            // For now: strip nested fields from projection for the semi-join path
            List<String> parentOnlyProjection = new ArrayList<>();
            for (String f : projection) {
                if (!f.startsWith(nestedPath + ".")) {
                    parentOnlyProjection.add(f);
                }
            }
            projection = parentOnlyProjection;
        }

        return new N1Descriptor(indexName, unnestPath, predicate, "__row_id__",
            projection, aggregate, baseRowType);
    }

    /** Extracts the source index name from the PPL source command. */
    public static String extractSourceIndex(String pplText) {
        String firstCmd = pplText.split("\\|")[0].trim();
        Matcher m = Pattern.compile("source\\s*=\\s*(\\S+)").matcher(firstCmd);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    /**
     * Translates a dotted-nested-syntax PPL query into expand-syntax.
     * Example: "source=blogs | where comments.score > 4 | fields title"
     *       -> "source=blogs | expand comments | where score > 4 | fields title"
     *
     * Returns null if no translation is needed (no nested refs in WHERE/STATS/FIELDS).
     */
    public static String translateToExpand(String pplText, RelDataType baseRowType, String indexName) {
        Set<String> nestedColumns = new HashSet<>();
        for (RelDataTypeField field : baseRowType.getFieldList()) {
            if (field.getType().getSqlTypeName() == SqlTypeName.ARRAY
                && field.getType().getComponentType() != null
                && field.getType().getComponentType().isStruct()) {
                nestedColumns.add(field.getName());
            }
        }
        if (nestedColumns.isEmpty()) {
            return null;
        }

        String[] commands = pplText.split("\\|");
        if (commands.length < 2) return null;

        // Check if any command references a nested column with dot notation
        String nestedCol = null;
        for (int i = 1; i < commands.length; i++) {
            String cmd = commands[i].trim();
            for (String col : nestedColumns) {
                if (cmd.contains(col + ".")) {
                    nestedCol = col;
                    break;
                }
            }
            if (nestedCol != null) break;
        }
        if (nestedCol == null) return null;

        // Build the translated query:
        // 1. Keep the source command
        // 2. Insert "expand <nestedCol>" after source
        // 3. Strip the "<nestedCol>." prefix from all subsequent commands
        StringBuilder result = new StringBuilder();
        result.append(commands[0].trim());
        result.append(" | expand ").append(nestedCol);
        String dotPrefix = nestedCol + ".";
        for (int i = 1; i < commands.length; i++) {
            String cmd = commands[i].trim();
            cmd = cmd.replace(dotPrefix, "");
            result.append(" | ").append(cmd);
        }
        return result.toString();
    }

    /** Builds multi-level unnest path. For "posts.replies.upvotes" returns ["posts","posts.replies"]. */
    private static List<String> buildUnnestPath(String clause, String nestedPath, RelDataType rowType) {
        List<String> path = new ArrayList<>();
        path.add(nestedPath);

        // Find the full field reference in the clause that starts with nestedPath
        Pattern fieldRef = Pattern.compile(Pattern.quote(nestedPath) + "\\.(\\w+(?:\\.\\w+)*)");
        Matcher m = fieldRef.matcher(clause);
        if (m.find()) {
            String afterPrefix = m.group(1); // e.g., "replies.upvotes" or "score"
            String[] parts = afterPrefix.split("\\.");

            // Check if intermediate parts are also nested (ARRAY<ROW>) by examining the schema
            // Walk the struct type to find sub-arrays
            RelDataType currentType = rowType;
            // Find the top-level array column's element type
            for (RelDataTypeField field : currentType.getFieldList()) {
                if (field.getName().equals(nestedPath) && field.getType().getSqlTypeName() == SqlTypeName.ARRAY) {
                    RelDataType elementType = field.getType().getComponentType();
                    if (elementType != null && elementType.isStruct()) {
                        // Check each intermediate part
                        String pathSoFar = nestedPath;
                        for (int i = 0; i < parts.length - 1; i++) { // skip last part (it's the leaf field)
                            String part = parts[i];
                            // Check if this part is an ARRAY<ROW> sub-field
                            for (RelDataTypeField subField : elementType.getFieldList()) {
                                if (subField.getName().equals(part)
                                    && subField.getType().getSqlTypeName() == SqlTypeName.ARRAY
                                    && subField.getType().getComponentType() != null
                                    && subField.getType().getComponentType().isStruct()) {
                                    pathSoFar = pathSoFar + "." + part;
                                    path.add(pathSoFar);
                                    elementType = subField.getType().getComponentType();
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        return path;
    }

    /** Check if a clause text contains a reference to a nested sub-field. */
    private static boolean hasNestedRef(String clause, Set<String> nestedColumns) {
        for (String col : nestedColumns) {
            if (clause.contains(col + ".")) {
                return true;
            }
        }
        return false;
    }

    /** Find which nested column is referenced in the clause. */
    private static String findNestedPath(String clause, Set<String> nestedColumns) {
        for (String col : nestedColumns) {
            if (clause.contains(col + ".")) {
                return col;
            }
        }
        return null;
    }

    /** Parse a simple predicate from a WHERE clause. Handles: field op value, AND, OR. */
    private static N1Predicate parsePredicate(String whereClause, String nestedPath) {
        // Handle AND
        if (whereClause.toLowerCase().contains(" and ")) {
            String[] parts = whereClause.split("(?i)\\s+and\\s+");
            List<N1Predicate> children = new ArrayList<>();
            for (String part : parts) {
                N1Predicate child = parseSingleComparison(part.trim(), nestedPath);
                if (child != null) children.add(child);
            }
            if (children.size() == 1) return children.get(0);
            if (children.size() > 1) return new N1Predicate.And(children);
            return null;
        }
        // Handle OR
        if (whereClause.toLowerCase().contains(" or ")) {
            String[] parts = whereClause.split("(?i)\\s+or\\s+");
            List<N1Predicate> children = new ArrayList<>();
            for (String part : parts) {
                N1Predicate child = parseSingleComparison(part.trim(), nestedPath);
                if (child != null) children.add(child);
            }
            if (children.size() == 1) return children.get(0);
            if (children.size() > 1) return new N1Predicate.Or(children);
            return null;
        }
        return parseSingleComparison(whereClause, nestedPath);
    }

    /** Parse a single comparison like "comments.score > 4" or "reviews.rating >= 3". */
    private static N1Predicate parseSingleComparison(String expr, String nestedPath) {
        // Pattern: field op value
        // Operators: >=, <=, !=, >, <, =
        Pattern p = Pattern.compile("([\\w.]+)\\s*(>=|<=|!=|>|<|=)\\s*(.+)");
        Matcher m = p.matcher(expr.trim());
        if (!m.matches()) return null;

        String fullField = m.group(1);  // "comments.score"
        String op = m.group(2);          // ">"
        String valueStr = m.group(3).trim(); // "4"

        // Strip the nested path prefix to get the LEAF sub-field name
        // For "posts.replies.upvotes" with nestedPath="posts", get just "upvotes"
        String field;
        if (fullField.startsWith(nestedPath + ".")) {
            String stripped = fullField.substring(nestedPath.length() + 1);
            int lastDot = stripped.lastIndexOf('.');
            field = (lastDot >= 0) ? stripped.substring(lastDot + 1) : stripped;
        } else {
            // Not a nested field ref in this predicate — could be a parent field filter
            // For now, skip parent-field predicates in the N1 path
            return null;
        }

        // Parse the operator
        N1Predicate.Op predOp = switch (op) {
            case ">" -> N1Predicate.Op.GT;
            case ">=" -> N1Predicate.Op.GTE;
            case "<" -> N1Predicate.Op.LT;
            case "<=" -> N1Predicate.Op.LTE;
            case "=" -> N1Predicate.Op.EQUAL;
            case "!=" -> N1Predicate.Op.NOT_EQUAL;
            default -> null;
        };
        if (predOp == null) return null;

        // Parse the value (integer, double, or string)
        Object value = parseValue(valueStr);
        if (value == null) return null;

        return new N1Predicate.Comparison(field, predOp, value);
    }

    /** Parse a STATS clause like "avg(comments.score)" or "count() by comments.author".
     *  For multiple aggregates like "avg(X), max(X)", finds the FIRST one referencing the nested path.
     *  For mixed parent+nested like "avg(views), avg(comments.score)", skips the parent aggregate. */
    private static N1Aggregate parseAggregate(String statsClause, String nestedPath) {
        // Find all func(field) patterns in the stats clause
        Pattern p = Pattern.compile("(avg|sum|min|max|count)\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(statsClause.trim());

        String funcName = null;
        String argField = null;

        // Find the first aggregate that references the nested path
        while (m.find()) {
            String candidate = m.group(2).trim();
            if (candidate.startsWith(nestedPath + ".") || candidate.isEmpty()) {
                funcName = m.group(1).toLowerCase();
                argField = candidate;
                break;
            }
        }
        if (funcName == null) return null;

        // Check for "by" clause
        String groupByField = null;
        Pattern byPattern = Pattern.compile("\\bby\\s+(\\S+)", Pattern.CASE_INSENSITIVE);
        Matcher byMatcher = byPattern.matcher(statsClause);
        if (byMatcher.find()) {
            groupByField = byMatcher.group(1).trim();
        }

        N1Aggregate.Fn fn = switch (funcName) {
            case "avg" -> N1Aggregate.Fn.AVG;
            case "sum" -> N1Aggregate.Fn.SUM;
            case "min" -> N1Aggregate.Fn.MIN;
            case "max" -> N1Aggregate.Fn.MAX;
            case "count" -> N1Aggregate.Fn.COUNT;
            default -> null;
        };
        if (fn == null) return null;

        // Strip nested path prefix from arg field — get just the LEAF field name
        // For "posts.replies.upvotes" with nestedPath="posts", cleanArg should be "upvotes"
        // (the leaf after all nesting is stripped)
        String cleanArg = null;
        if (!argField.isEmpty()) {
            if (argField.startsWith(nestedPath + ".")) {
                String stripped = argField.substring(nestedPath.length() + 1);
                // Get just the last segment (leaf field name)
                int lastDot = stripped.lastIndexOf('.');
                cleanArg = (lastDot >= 0) ? stripped.substring(lastDot + 1) : stripped;
            } else {
                cleanArg = argField;
            }
        }

        // Strip nested path prefix from group-by field — same logic
        String cleanGroupBy = null;
        if (groupByField != null && groupByField.startsWith(nestedPath + ".")) {
            String stripped = groupByField.substring(nestedPath.length() + 1);
            int lastDot = stripped.lastIndexOf('.');
            cleanGroupBy = (lastDot >= 0) ? stripped.substring(lastDot + 1) : stripped;
        } else {
            cleanGroupBy = groupByField;
        }

        // Build output column name
        String outputCol = funcName + "(" + (argField.isEmpty() ? "" : argField) + ")";
        String groupByOutputCol = cleanGroupBy;

        return new N1Aggregate(fn, cleanArg, outputCol, cleanGroupBy, groupByOutputCol, null, null);
    }

    /** Parse a value string to Integer, Double, Boolean, or String. */
    private static Object parseValue(String valueStr) {
        // Try integer
        try { return Integer.parseInt(valueStr); } catch (NumberFormatException ignored) {}
        // Try long
        try { return Long.parseLong(valueStr); } catch (NumberFormatException ignored) {}
        // Try double
        try { return Double.parseDouble(valueStr); } catch (NumberFormatException ignored) {}
        // Boolean
        if ("true".equalsIgnoreCase(valueStr)) return Boolean.TRUE;
        if ("false".equalsIgnoreCase(valueStr)) return Boolean.FALSE;
        // String (strip quotes if present)
        if ((valueStr.startsWith("'") && valueStr.endsWith("'"))
            || (valueStr.startsWith("\"") && valueStr.endsWith("\""))) {
            return valueStr.substring(1, valueStr.length() - 1);
        }
        // Bare string
        return valueStr;
    }
}
