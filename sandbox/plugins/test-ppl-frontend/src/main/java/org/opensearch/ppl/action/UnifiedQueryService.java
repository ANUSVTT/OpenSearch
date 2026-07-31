/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Core orchestrator: PPL text → RelNode → QueryPlanExecutor → PPLResponse.
 *
 * <p>Passes the logical RelNode directly to the back-end engine (e.g. DataFusion)
 * which handles optimization and execution natively via Substrait. No Janino
 * code generation needed.
 */
public class UnifiedQueryService {

    private static final Logger logger = LogManager.getLogger(UnifiedQueryService.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;
    private final EngineContextProvider contextProvider;

    public UnifiedQueryService(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor, EngineContextProvider contextProvider) {
        this.planExecutor = planExecutor;
        this.contextProvider = contextProvider;
    }

    /**
     * Executes a PPL query through the simplified pipeline:
     * PPL text → RelNode → planExecutor.execute() → PPLResponse.
     */
    public PPLResponse execute(String pplText) {
        return execute(pplText, false);
    }

    /**
     * Executes a PPL query with profiling: PPL text → RelNode →
     * planExecutor.executeWithProfile() → PPLResponse with profile.
     */
    public PPLResponse executeWithProfile(String pplText) {
        return execute(pplText, true);
    }

    private PPLResponse execute(String pplText, boolean profile) {
        // Wrap the SchemaPlus in a delegating AbstractSchema that preserves lazy table resolution.
        // The underlying OpenSearchSchemaBuilder resolves wildcard/comma/exclusion expressions
        // lazily via getTable(name) — a static copy would lose that.
        SchemaPlus schemaPlus = contextProvider.getContext().schema();
        AbstractSchema delegatingSchema = new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return new HashMap<>() {
                    {
                        for (String tableName : schemaPlus.getTableNames()) {
                            super.put(tableName, schemaPlus.getTable(tableName));
                        }
                    }

                    @Override
                    public Table get(Object key) {
                        Table t = super.get(key);
                        if (t == null && key instanceof String name) {
                            t = schemaPlus.getTable(name);
                            if (t != null) super.put(name, t);
                        }
                        return t;
                    }
                };
            }
        };

        logger.info(
            "[UnifiedQueryService] schemaPlus class: {}, tableNames: {}, contextProvider class: {}",
            schemaPlus.getClass().getName(),
            schemaPlus.getTableNames(),
            contextProvider.getClass().getName()
        );

        try (
            UnifiedQueryContext context = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog(DEFAULT_CATALOG, delegatingSchema)
                .defaultNamespace(DEFAULT_CATALOG)
                // The unified PPL parser reuses the v2 AstBuilder, which gates Calcite-only
                // commands (table, regex, rex, convert) on plugins.calcite.enabled. The unified
                // path is by definition Calcite-based — flag it on so those commands lower
                // through the same Project/Filter RelNodes as their non-aliased counterparts.
                .setting("plugins.calcite.enabled", true)
                .build()
        ) {

            // Log what the context's root schema looks like
            logger.info("[UnifiedQueryService] Context built, planning PPL: {}", pplText);
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

            // [NESTED] Nested-field queries are planned normally: PPL `expand <array>` lowers to a
            // Calcite Correlate+Uncollect that OpenSearchNestedFieldRewriter marks and isthmus emits as
            // an ExtensionSingleRel (see DataFusionFragmentConvertor). No special-casing here — the
            // generic path handles filter/aggregate/group/sort/projection uniformly.
            //
            // Safety net: if the upstream PPL validator still rejects a dotted nested predicate (e.g.
            // `where comments.score > 4`) with "Unsupported conversion for Relational Data type: ROW"
            // (the sql-plugin's ITEM-on-ROW fix not yet in the resolved plugin version), auto-translate
            // to expand form and retry rather than failing the query outright.
            RelNode logicalPlan;
            try {
                logicalPlan = planner.plan(pplText);
            } catch (Exception e) {
                String msg = e.getMessage() != null ? e.getMessage() : "";
                Throwable cause = e.getCause();
                String causeMsg = cause != null && cause.getMessage() != null ? cause.getMessage() : "";
                if (msg.contains("Unsupported conversion for Relational Data type")
                    || causeMsg.contains("Unsupported conversion for Relational Data type")) {
                    String expanded = tryInjectExpand(pplText, schemaPlus);
                    if (expanded != null) {
                        logger.info("[NESTED] dotted predicate rejected; retrying with expand: {}", expanded);
                        logicalPlan = planner.plan(expanded);
                    } else {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("[NESTED] logical plan for PPL [{}]:\n{}", pplText, org.apache.calcite.plan.RelOptUtil.toString(logicalPlan));
            }

            // Extract column names from the RelNode's row type.
            List<RelDataTypeField> fields = logicalPlan.getRowType().getFieldList();
            List<String> columns = new ArrayList<>(fields.size());
            for (RelDataTypeField field : fields) {
                columns.add(field.getName());
            }

            QueryRequestContext baseCtx = contextProvider.getContext();
            QueryRequestContext queryCtx = new QueryRequestContext(
                baseCtx.clusterState(),
                baseCtx.schema(),
                pplText,
                baseCtx.parentTask()
            );

            if (profile) {
                PlainActionFuture<ProfiledResult> future = new PlainActionFuture<>();
                planExecutor.executeWithProfile(logicalPlan, queryCtx, future);
                ProfiledResult result = future.actionGet();

                if (result.isSuccess() == false) {
                    Throwable failure = result.failure();
                    if (failure instanceof RuntimeException re) throw re;
                    throw new RuntimeException("Query failed: " + failure.getMessage(), failure);
                }

                List<Object[]> rows = new ArrayList<>();
                for (Object[] row : result.rows()) {
                    rows.add(row);
                }
                return new PPLResponse(columns, rows, result.profile());
            }

            // Non-profile path: use execute() directly so exception conversion
            // (e.g. CircuitBreakingException) is handled by DefaultPlanExecutor's
            // convertingListener without being wrapped in ProfiledResult.
            PlainActionFuture<Iterable<Object[]>> future = new PlainActionFuture<>();
            planExecutor.execute(logicalPlan, queryCtx, future);
            Iterable<Object[]> results = future.actionGet();

            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : results) {
                rows.add(row);
            }
            return new PPLResponse(columns, rows);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to execute PPL query: " + e.getMessage(), e);
        }
    }

    // ── [NESTED] Dotted-to-expand auto-translation (bridges upstream validator gap) ──

    /**
     * Given PPL text containing dotted nested refs (e.g. {@code where comments.score > 4}), detect
     * which top-level fields are ARRAY-typed (i.e. nested) in the schema, and rewrite:
     * <pre>
     *   source=blogs | where comments.score > 4 | fields title
     *     ->  source=blogs | expand comments | where score > 4 | fields title
     * </pre>
     * Returns {@code null} if no nested field is detected (caller should rethrow the original error).
     */
    private static String tryInjectExpand(String pplText, SchemaPlus schemaPlus) {
        // 1. Extract the index name from "source=<index>"
        Matcher sourceMatcher = Pattern.compile("source\\s*=\\s*(\\S+)").matcher(pplText);
        if (sourceMatcher.find() == false) return null;
        String indexName = sourceMatcher.group(1);

        // 2. Resolve the table's fields and find which are ARRAY-typed (nested)
        Table table = schemaPlus.getTable(indexName);
        if (table == null) return null;
        org.apache.calcite.rel.type.RelDataType rowType = table.getRowType(
            new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
        Set<String> nestedFields = new HashSet<>();
        for (org.apache.calcite.rel.type.RelDataTypeField f : rowType.getFieldList()) {
            if (f.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.ARRAY) {
                nestedFields.add(f.getName());
            }
        }
        if (nestedFields.isEmpty()) return null;

        // 3. Split the PPL on "|" and scan for dotted refs matching nested fields
        String[] segments = pplText.split("\\|");
        String nestedFieldUsed = null;
        for (int i = 1; i < segments.length; i++) {
            String seg = segments[i].trim();
            for (String nf : nestedFields) {
                if (seg.contains(nf + ".")) {
                    nestedFieldUsed = nf;
                    break;
                }
            }
            if (nestedFieldUsed != null) break;
        }
        if (nestedFieldUsed == null) return null;

        // 4. Insert "| expand <nestedField>" after source, and strip the prefix from dotted refs
        StringBuilder result = new StringBuilder();
        result.append(segments[0].trim());
        result.append(" | expand ").append(nestedFieldUsed);
        String prefix = nestedFieldUsed + ".";
        for (int i = 1; i < segments.length; i++) {
            String seg = segments[i].trim();
            seg = seg.replace(prefix, "");
            result.append(" | ").append(seg);
        }
        return result.toString();
    }
}
