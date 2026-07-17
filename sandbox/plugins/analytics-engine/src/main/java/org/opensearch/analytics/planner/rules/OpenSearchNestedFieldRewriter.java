/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * POC nested (N1): Pre-marking rewriter that detects {@code ITEM($N, 'field')} expressions
 * in a {@link LogicalProject} where {@code $N} references an ARRAY(ROW(...)) column, and
 * rewrites the tree to inject a {@link LogicalCorrelate} + {@link Uncollect} (UNNEST) so
 * that the nested array is exploded into flat rows before the project.
 *
 * <p>This runs BEFORE the mark() phase so that {@code OpenSearchProjectRule} never sees
 * the unsupported {@code ITEM} function — it only sees simple {@code $N} column references
 * to the flattened struct fields.
 *
 * <p>Rewrite example:
 * <pre>
 * BEFORE:
 *   LogicalProject(title=[$1], comments.author=[ITEM($0, 'author')])
 *     LogicalTableScan(table=[[opensearch, blogs]])
 *
 * AFTER:
 *   LogicalProject(title=[$1], author=[$3])
 *     LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
 *       LogicalTableScan(table=[[opensearch, blogs]])
 *       Uncollect
 *         LogicalProject(comments=[$cor0.comments])
 *           LogicalValues(tuples=[[{ 0 }]])
 * </pre>
 *
 * @opensearch.internal
 */
public final class OpenSearchNestedFieldRewriter {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchNestedFieldRewriter.class);

    private OpenSearchNestedFieldRewriter() {}

    /**
     * Attempts to rewrite the RelNode tree if the top-level (or any) LogicalProject contains
     * ITEM calls on ARRAY-typed columns. Returns the rewritten tree, or the original if no
     * rewrite was needed.
     */
    public static RelNode rewrite(RelNode root) {
        if (root instanceof LogicalProject) {
            Optional<RelNode> rewritten = rewriteProject((LogicalProject) root);
            if (rewritten.isPresent()) {
                return rewritten.get();
            }
        }
        // TODO: for a full implementation, walk the tree recursively to find nested Projects.
        // For POC, we only handle the top-level Project case.
        return root;
    }

    /**
     * Core rewrite logic: given a LogicalProject, detect ITEM calls on ARRAY columns,
     * inject Correlate+Uncollect, and adjust references.
     */
    private static Optional<RelNode> rewriteProject(LogicalProject project) {
        RelNode input = project.getInput();
        RelDataType inputRowType = input.getRowType();
        List<RexNode> projectExprs = project.getProjects();

        // 1. Scan project expressions for ITEM($N, 'fieldName') where $N is ARRAY type
        // Group by the array column index (all ITEM calls on same array share one UNNEST)
        Map<Integer, List<ItemAccess>> itemAccessesByArrayCol = new LinkedHashMap<>();

        for (int i = 0; i < projectExprs.size(); i++) {
            RexNode expr = projectExprs.get(i);
            Optional<ItemAccess> access = extractItemAccess(expr, inputRowType);
            if (access.isPresent()) {
                ItemAccess ia = access.get();
                itemAccessesByArrayCol.computeIfAbsent(ia.arrayColIndex, k -> new ArrayList<>()).add(ia);
            }
        }

        if (itemAccessesByArrayCol.isEmpty()) {
            return Optional.empty(); // No ITEM calls on ARRAY columns — nothing to rewrite
        }

        LOGGER.info("[NESTED-POC] OpenSearchNestedFieldRewriter: found {} ITEM accesses on {} array column(s)",
            itemAccessesByArrayCol.values().stream().mapToInt(List::size).sum(),
            itemAccessesByArrayCol.size());

        // For POC: handle the case of exactly ONE array column being unnested
        // (multiple array UNNESTs in one query would need multiple correlates — deferred)
        if (itemAccessesByArrayCol.size() > 1) {
            LOGGER.warn("[NESTED-POC] Multiple array columns referenced — POC only supports one. Skipping rewrite.");
            return Optional.empty();
        }

        Map.Entry<Integer, List<ItemAccess>> entry = itemAccessesByArrayCol.entrySet().iterator().next();
        int arrayColIndex = entry.getKey();
        List<ItemAccess> accesses = entry.getValue();

        RelOptCluster cluster = project.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        // 2. Get the array column's element (struct) type
        RelDataTypeField arrayField = inputRowType.getFieldList().get(arrayColIndex);
        RelDataType arrayType = arrayField.getType(); // ARRAY(ROW(author:VARCHAR, score:INTEGER))
        RelDataType elementType = arrayType.getComponentType(); // ROW(author:VARCHAR, score:INTEGER)

        if (elementType == null) {
            LOGGER.warn("[NESTED-POC] Array column {} has no component type, skipping rewrite", arrayField.getName());
            return Optional.empty();
        }

        LOGGER.info("[NESTED-POC] Rewriting: array column '{}' (index={}) with element type: {}",
            arrayField.getName(), arrayColIndex, elementType);

        // 3. Build the Correlate + Uncollect structure
        // This mirrors what CalciteRelNodeVisitor.buildExpandRelNode does:
        //   LEFT: the original input (TableScan)
        //   RIGHT: Uncollect(Project($cor0.arrayCol, LogicalValues(oneRow)))
        //   JOIN: LogicalCorrelate(INNER)

        // Create correlation variable
        CorrelationId correlId = cluster.createCorrel();
        RelDataType correlType = input.getRowType();
        RexNode correlVariable = rexBuilder.makeCorrel(correlType, correlId);

        // Access the array field from the correlation variable
        RexNode correlArrayAccess = rexBuilder.makeFieldAccess(correlVariable, arrayColIndex);

        // Build the right side: Uncollect(Project($cor0.arrayCol, OneRow))
        RelNode oneRow = LogicalValues.createOneRow(cluster);

        // Project the correlated array field access
        RelNode rightProject = LogicalProject.create(
            oneRow,
            List.of(),  // hints
            List.of(correlArrayAccess),
            List.of(arrayField.getName())
        );

        // Uncollect (UNNEST) — this explodes the array into rows
        // The output type will be the struct fields: (author:VARCHAR, score:INTEGER)
        RelNode uncollect = Uncollect.create(rightProject.getTraitSet(), rightProject, false, List.of());

        LOGGER.info("[NESTED-POC] Uncollect output row type: {}", uncollect.getRowType());

        // Build the LogicalCorrelate (INNER join between input and uncollect)
        ImmutableBitSet requiredColumns = ImmutableBitSet.of(arrayColIndex);
        LogicalCorrelate correlate = LogicalCorrelate.create(
            input,
            uncollect,
            List.of(),  // hints
            correlId,
            requiredColumns,
            JoinRelType.INNER
        );

        LOGGER.info("[NESTED-POC] Correlate output row type: {}", correlate.getRowType());

        // 4. Build the new Project on top of the Correlate
        // The correlate's row type is: [original_cols..., unnested_struct_fields...]
        // Original columns: indices 0..inputRowType.getFieldCount()-1
        // Unnested fields: indices inputRowType.getFieldCount()..end
        int originalColCount = inputRowType.getFieldCount();
        RelDataType correlateRowType = correlate.getRowType();

        // Build a map from struct field name → index in correlate output
        Map<String, Integer> unnestedFieldIndexMap = new HashMap<>();
        List<RelDataTypeField> correlateFields = correlateRowType.getFieldList();
        for (int i = originalColCount; i < correlateFields.size(); i++) {
            unnestedFieldIndexMap.put(correlateFields.get(i).getName(), i);
        }

        LOGGER.info("[NESTED-POC] Unnested field index map: {}", unnestedFieldIndexMap);

        // Now rewrite each project expression:
        // - ITEM($N, 'fieldName') → $unnestedFieldIndex (where fieldName maps to the unnested col)
        // - $N (non-array) → $N (unchanged, same position in correlate)
        List<RexNode> newProjectExprs = new ArrayList<>(projectExprs.size());
        List<String> newFieldNames = new ArrayList<>(project.getRowType().getFieldNames());

        for (int i = 0; i < projectExprs.size(); i++) {
            RexNode expr = projectExprs.get(i);
            Optional<ItemAccess> access = extractItemAccess(expr, inputRowType);
            if (access.isPresent()) {
                ItemAccess ia = access.get();
                Integer unnestedIdx = unnestedFieldIndexMap.get(ia.fieldName);
                if (unnestedIdx != null) {
                    // Replace ITEM($0, 'author') with $unnestedIdx (direct reference to unnested field)
                    RelDataType fieldType = correlateRowType.getFieldList().get(unnestedIdx).getType();
                    newProjectExprs.add(rexBuilder.makeInputRef(fieldType, unnestedIdx));
                    LOGGER.info("[NESTED-POC] Rewrote ITEM(${},'{}') → ${} (type={})",
                        ia.arrayColIndex, ia.fieldName, unnestedIdx, fieldType);
                } else {
                    // Field not found in unnested output — keep original (will fail later but shouldn't happen)
                    LOGGER.warn("[NESTED-POC] Field '{}' not found in unnested output, keeping ITEM expression", ia.fieldName);
                    newProjectExprs.add(expr);
                }
            } else if (expr instanceof RexInputRef ref) {
                // Simple column reference — keep same index (still valid in correlate's output)
                RelDataType fieldType = correlateRowType.getFieldList().get(ref.getIndex()).getType();
                newProjectExprs.add(rexBuilder.makeInputRef(fieldType, ref.getIndex()));
            } else {
                // Other expressions — keep as-is (may need adjustment for complex cases)
                newProjectExprs.add(expr);
            }
        }

        // Build the output row type for the new project — derive from actual expression types
        // (can't reuse project.getRowType() because ITEM returned ROW but $3 is VARCHAR)
        List<String> fieldNames = project.getRowType().getFieldNames();
        RelNode newProject = LogicalProject.create(
            correlate,
            List.of(),  // hints
            newProjectExprs,
            fieldNames
        );

        LOGGER.info("[NESTED-POC] Rewrite complete. New plan:\n{}", RelOptUtil.toString(newProject));

        return Optional.of(newProject);
    }

    /**
     * Extracts an ITEM access pattern from a RexNode: ITEM($N, 'fieldName') where $N
     * references an ARRAY-typed column.
     */
    private static Optional<ItemAccess> extractItemAccess(RexNode expr, RelDataType inputRowType) {
        if (!(expr instanceof RexCall call)) {
            return Optional.empty();
        }
        // ITEM function has name "ITEM" and 2 operands: (array_ref, field_name_literal)
        if (!"ITEM".equals(call.getOperator().getName()) || call.getOperands().size() != 2) {
            return Optional.empty();
        }
        RexNode arrayRef = call.getOperands().get(0);
        RexNode fieldNameNode = call.getOperands().get(1);

        // Array reference must be a simple column reference ($N)
        if (!(arrayRef instanceof RexInputRef inputRef)) {
            return Optional.empty();
        }
        // Field name must be a string literal
        if (!(fieldNameNode instanceof RexLiteral literal) || literal.getTypeName() != SqlTypeName.CHAR) {
            return Optional.empty();
        }

        int colIndex = inputRef.getIndex();
        RelDataType colType = inputRowType.getFieldList().get(colIndex).getType();

        // Check that the column is an ARRAY type
        if (colType.getSqlTypeName() != SqlTypeName.ARRAY) {
            return Optional.empty();
        }

        String fieldName = literal.getValueAs(String.class);
        return Optional.of(new ItemAccess(colIndex, fieldName));
    }

    /** Holds information about a detected ITEM($arrayColIndex, 'fieldName') expression. */
    private record ItemAccess(int arrayColIndex, String fieldName) {}
}
