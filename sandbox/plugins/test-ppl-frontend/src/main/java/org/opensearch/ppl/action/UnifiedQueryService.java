/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.plan.RelOptUtil;
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
import java.util.List;
import java.util.Map;

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
        SchemaPlus schemaPlus = contextProvider.getContext().schema(); // this needs the nested doc
        System.out.println("shreanu uqs : " + pplText);
        System.out.println("shreanu schemaPlus : " + schemaPlus.toString());
        System.out.println("[NESTED-POC] UnifiedQueryService.execute() called with query: " + pplText);
        System.out.println("[NESTED-POC] SchemaPlus tableNames: " + schemaPlus.getTableNames());

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

            System.out.println("shreanu Stop 2");
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

            // [NESTED] Auto-detect nested field references in WHERE/STATS and translate
            // dotted syntax (comments.score > 4) to expand syntax (expand comments | where score > 4).
            // The expand command makes nested fields flat columns that the PPL type-checker accepts,
            // and the OpenSearchNestedFieldRewriter handles the Correlate+Uncollect injection.
            String effectivePpl = pplText;
            String indexName = N1QueryAnalyzer.extractSourceIndex(pplText);
            if (indexName != null) {
                try {
                    RelNode baseScan = planner.plan("source=" + indexName);
                    String translated = N1QueryAnalyzer.translateToExpand(pplText, baseScan.getRowType(), indexName);
                    if (translated != null) {
                        logger.info("[NESTED] Translated dotted nested query to expand syntax: {} -> {}",
                            pplText, translated);
                        effectivePpl = translated;
                    }
                } catch (Exception e) {
                    logger.debug("[NESTED] Translation failed, using original query", e);
                }
            }

            RelNode logicalPlan = planner.plan(effectivePpl);
            //  The UnifiedQueryPlanner (built on Apache Calcite) has a PPL grammar that maps each PPL pipe command
            //  to the corresponding Calcite LogicalXxx node. This is hardcoded in the parser:

            System.out.println("shreanu Stop 3");

            // ===== NESTED-POC LOGGING =====
            logger.info("[NESTED-POC] ====== UnifiedQueryPlanner.plan() OUTPUT ======");
            logger.info("[NESTED-POC] Query: {}", pplText);
            logger.info("[NESTED-POC] RelNode class: {}", logicalPlan.getClass().getSimpleName());
            logger.info("[NESTED-POC] RelNode row type: {}", logicalPlan.getRowType());
            logger.info("[NESTED-POC] RelNode tree:\n{}", org.apache.calcite.plan.RelOptUtil.toString(logicalPlan));
            for (RelDataTypeField field : logicalPlan.getRowType().getFieldList()) {
                logger.info("[NESTED-POC]   Column: {} -> type: {} (sqlTypeName: {})",
                    field.getName(), field.getType(), field.getType().getSqlTypeName());
            }
            logger.info("[NESTED-POC] ====== END UnifiedQueryPlanner OUTPUT ======");

            // Extract column names from the RelNode's row type
            List<RelDataTypeField> fields = logicalPlan.getRowType().getFieldList();
            List<String> columns = new ArrayList<>(fields.size());
            for (RelDataTypeField field : fields) {
                columns.add(field.getName());
            }

            QueryRequestContext baseCtx = contextProvider.getContext();
            QueryRequestContext queryCtx = new QueryRequestContext(baseCtx.clusterState(), baseCtx.schema(), pplText,
                null, null);

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
            System.out.println("shreanu exeutor step logicalPlan : " + logicalPlan + " and queryCtx : " + queryCtx);
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
}
