/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * POC nested (N1): Pre-Substrait rewriter that detects {@code ITEM($N, 'field')} expressions
 * in a {@link LogicalProject} where {@code $N} is an ARRAY(ROW(...)) column, strips those ITEM
 * calls (replacing with plain column refs to the scan), and records the unnest info so that
 * the Substrait post-processor can inject ExtensionSingleRel(unnest).
 *
 * <p>This replaces the earlier approach of injecting Correlate+Uncollect in the planner
 * (which caused the CBO to insert an unnecessary Exchange). Instead, ITEM() calls pass
 * through mark()+CBO unchanged (CBO sees a simple Project→Scan, no Exchange needed),
 * and only here — right before Substrait serialization — do we detect and handle them.
 *
 * @opensearch.internal
 */
final class CorrelateUncollectRewriter {

    private static final Logger LOGGER = LogManager.getLogger(CorrelateUncollectRewriter.class);

    /** Thread-local to communicate unnest info to the Substrait post-processor. */
    private static final ThreadLocal<UnnestInfo> UNNEST_INFO = new ThreadLocal<>();

    private CorrelateUncollectRewriter() {}

    /** Info about the detected unnest — needed by the Substrait post-processor. */
    record UnnestInfo(
        String arrayColumnName,
        int arrayColumnIndex,
        List<String> structFieldNames,
        /** For each output column: if >= 0, it's a struct field index; if -1, it's a scan column. */
        int[] unnestFieldIndices,
        int[] scanColIndices
    ) {}

    /** Returns the unnest info detected during the last rewrite, or null if none. */
    static UnnestInfo getUnnestInfo() {
        return UNNEST_INFO.get();
    }

    /** Clears the thread-local after the convertor has consumed it. */
    static void clearUnnestInfo() {
        UNNEST_INFO.remove();
    }

    /**
     * Detects ITEM($N, 'field') expressions where $N is an ARRAY(ROW(...)) column.
     * Strips them from the project (replaces with plain scan column references),
     * stores UnnestInfo for the Substrait post-processor, and returns the modified plan.
     *
     * If no ITEM calls on ARRAY columns are found, returns the input unchanged.
     */
    static RelNode rewrite(RelNode root) {
        // PATH A: Handle Correlate+Uncollect that PPL's "expand" command already injected.
        // The tree looks like: Project → Correlate(Scan, Uncollect) or Filter → Correlate(Scan, Uncollect)
        RelNode correlateResult = tryRewriteExistingCorrelate(root);
        if (correlateResult != null) {
            return correlateResult;
        }

        // PATH B: Handle ITEM/array_element on ARRAY columns (from "fields comments.author")
        if (!(root instanceof LogicalProject project)) {
            return root;
        }

        RelNode input = project.getInput();
        RelDataType inputRowType = input.getRowType();
        List<RexNode> exprs = project.getProjects();

        // Scan for ITEM($N, 'field') or array_element($N, CAST('field')) where $N is ARRAY type.
        // The ITEM operator may have been rewritten to "array_element" by ItemTypeRebuilder
        // before reaching this point.
        Map<Integer, List<ItemAccess>> itemsByArrayCol = new LinkedHashMap<>();
        for (int i = 0; i < exprs.size(); i++) {
            RexNode expr = exprs.get(i);
            if (expr instanceof RexCall call && call.getOperands().size() == 2) {
                String opName = call.getOperator().getName();
                if ("ITEM".equals(opName) || "array_element".equalsIgnoreCase(opName)) {
                    RexNode arrayRef = call.getOperands().get(0);
                    RexNode fieldNameNode = call.getOperands().get(1);
                    // For ITEM: fieldNameNode is a CHAR literal like 'author'
                    // For array_element: fieldNameNode is CAST('author'):BIGINT — extract the inner literal
                    String fieldName = extractFieldName(fieldNameNode);
                    if (arrayRef instanceof RexInputRef ref && fieldName != null) {
                        int colIdx = ref.getIndex();
                        RelDataType colType = inputRowType.getFieldList().get(colIdx).getType();
                        if (colType.getSqlTypeName() == SqlTypeName.ARRAY) {
                            itemsByArrayCol.computeIfAbsent(colIdx, k -> new ArrayList<>())
                                .add(new ItemAccess(i, fieldName));
                        }
                    }
                }
            }
        }

        if (itemsByArrayCol.isEmpty()) {
            return root; // No ITEM on ARRAY — nothing to do
        }

        // POC: handle single array column
        if (itemsByArrayCol.size() > 1) {
            LOGGER.warn("[NESTED-POC] Multiple array columns with ITEM — POC supports only one. Skipping.");
            return root;
        }

        Map.Entry<Integer, List<ItemAccess>> entry = itemsByArrayCol.entrySet().iterator().next();
        int arrayColIndex = entry.getKey();
        List<ItemAccess> accesses = entry.getValue();

        RelDataTypeField arrayField = inputRowType.getFieldList().get(arrayColIndex);
        RelDataType elementType = arrayField.getType().getComponentType();
        if (elementType == null || !elementType.isStruct()) {
            return root;
        }

        List<String> structFieldNames = elementType.getFieldList().stream()
            .map(RelDataTypeField::getName).collect(Collectors.toList());

        LOGGER.info("[NESTED-POC] CorrelateUncollectRewriter: detected {} ITEM accesses on array column '{}' (index={})",
            accesses.size(), arrayField.getName(), arrayColIndex);

        // Build the output mapping
        int outputCount = exprs.size();
        int[] unnestFieldIndices = new int[outputCount];
        int[] scanColIndices = new int[outputCount];

        for (int i = 0; i < outputCount; i++) {
            unnestFieldIndices[i] = -1;
            scanColIndices[i] = -1;
        }

        // Map ITEM accesses to struct field indices
        for (ItemAccess access : accesses) {
            int structIdx = structFieldNames.indexOf(access.fieldName);
            if (structIdx >= 0) {
                unnestFieldIndices[access.projectIndex] = structIdx;
                LOGGER.info("[NESTED-POC] CorrelateUncollectRewriter: output[{}] → unnested field '{}' (struct idx={})",
                    access.projectIndex, access.fieldName, structIdx);
            }
        }

        // Map non-ITEM expressions to scan column indices
        for (int i = 0; i < outputCount; i++) {
            if (unnestFieldIndices[i] >= 0) continue; // already mapped as unnest field
            RexNode expr = exprs.get(i);
            if (expr instanceof RexInputRef ref) {
                scanColIndices[i] = ref.getIndex();
                LOGGER.info("[NESTED-POC] CorrelateUncollectRewriter: output[{}] → scan column {} ('{}')",
                    i, ref.getIndex(), inputRowType.getFieldList().get(ref.getIndex()).getName());
            }
        }

        // Store UnnestInfo for the post-processor
        UNNEST_INFO.set(new UnnestInfo(arrayField.getName(), arrayColIndex, structFieldNames,
            unnestFieldIndices, scanColIndices));

        // Return the input (scan) directly — strip the Project entirely.
        // The post-processor will build ReadRel → ExtensionSingleRel → ProjectRel in Substrait.
        LOGGER.info("[NESTED-POC] CorrelateUncollectRewriter: stripped Project with ITEM calls. " +
            "Post-processor will build ExtensionSingleRel('unnest:{}') + ProjectRel.",
            arrayField.getName());

        return input;
    }

    /**
     * PATH A: Detects a Correlate+Uncollect already in the tree (from PPL "expand" command).
     * Extracts the array column name, stores UnnestInfo, returns the scan.
     * Returns null if no Correlate+Uncollect found.
     */
    private static RelNode tryRewriteExistingCorrelate(RelNode root) {
        // Walk through Project/Filter to find a Correlate
        org.apache.calcite.rel.core.Correlate correlate = findCorrelate(root);
        if (correlate == null) return null;

        RelNode left = correlate.getLeft();  // the scan
        RelNode right = correlate.getRight(); // Uncollect → Project → Values

        if (!(right instanceof org.apache.calcite.rel.core.Uncollect uncollect)) return null;

        // Get the Uncollect's input to find which array column is being exploded
        RelNode uncollectInput = uncollect.getInput();
        if (!(uncollectInput instanceof LogicalProject correlatedProject)) return null;

        java.util.List<RexNode> correlatedExprs = correlatedProject.getProjects();
        if (correlatedExprs.isEmpty()) return null;

        // Extract the array column name from $cor0.comments
        RexNode correlatedExpr = correlatedExprs.get(0);
        String arrayColName = null;
        int arrayColIndex = -1;
        if (correlatedExpr instanceof org.apache.calcite.rex.RexFieldAccess fieldAccess) {
            arrayColName = fieldAccess.getField().getName();
            arrayColIndex = fieldAccess.getField().getIndex();
        }
        if (arrayColName == null) return null;

        LOGGER.info("[NESTED-POC] CorrelateUncollectRewriter PATH A: detected existing Correlate+Uncollect on column '{}' (index={})",
            arrayColName, arrayColIndex);

        // Get the struct fields from the Uncollect output type
        RelDataType uncollectOutputType = uncollect.getRowType();
        java.util.List<String> structFieldNames = uncollectOutputType.getFieldList().stream()
            .map(RelDataTypeField::getName).collect(java.util.stream.Collectors.toList());

        // Determine the output mapping from the root node
        // The root could be: Project(Correlate) or Filter(Correlate) or just Correlate
        int leftFieldCount = left.getRowType().getFieldCount();
        int[] unnestFieldIndices;
        int[] scanColIndices;

        if (root instanceof LogicalProject topProject) {
            int outputCount = topProject.getProjects().size();
            unnestFieldIndices = new int[outputCount];
            scanColIndices = new int[outputCount];
            for (int i = 0; i < outputCount; i++) {
                unnestFieldIndices[i] = -1;
                scanColIndices[i] = -1;
                RexNode expr = topProject.getProjects().get(i);
                if (expr instanceof RexInputRef ref) {
                    int idx = ref.getIndex();
                    if (idx >= leftFieldCount) {
                        unnestFieldIndices[i] = idx - leftFieldCount;
                        LOGGER.info("[NESTED-POC] PATH A: output[{}] → unnested field '{}' (struct idx={})",
                            i, structFieldNames.get(idx - leftFieldCount), idx - leftFieldCount);
                    } else {
                        scanColIndices[i] = idx;
                        LOGGER.info("[NESTED-POC] PATH A: output[{}] → scan column {} ('{}')",
                            i, idx, left.getRowType().getFieldList().get(idx).getName());
                    }
                }
            }
        } else {
            // No project on top — output all columns (left + unnested)
            int totalCols = leftFieldCount + structFieldNames.size();
            unnestFieldIndices = new int[totalCols];
            scanColIndices = new int[totalCols];
            for (int i = 0; i < totalCols; i++) {
                if (i < leftFieldCount) {
                    unnestFieldIndices[i] = -1;
                    scanColIndices[i] = i;
                } else {
                    unnestFieldIndices[i] = i - leftFieldCount;
                    scanColIndices[i] = -1;
                }
            }
        }

        UNNEST_INFO.set(new UnnestInfo(arrayColName, arrayColIndex, structFieldNames,
            unnestFieldIndices, scanColIndices));

        LOGGER.info("[NESTED-POC] CorrelateUncollectRewriter PATH A: stripped Correlate+Uncollect. " +
            "Post-processor will build ExtensionSingleRel('unnest:{}').", arrayColName);

        // Return the left side (the scan) — strip everything above
        return left;
    }

    /** Find a Correlate node by walking through Project/Filter wrappers. */
    private static org.apache.calcite.rel.core.Correlate findCorrelate(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Correlate c) return c;
        if (node instanceof LogicalProject p) return findCorrelate(p.getInput());
        if (node instanceof org.apache.calcite.rel.logical.LogicalFilter f) return findCorrelate(f.getInput());
        if (node instanceof org.apache.calcite.rel.logical.LogicalSort s) return findCorrelate(s.getInput());
        return null;
    }

    /** Extracts a field name from a literal or CAST(literal) expression. */
    private static String extractFieldName(RexNode node) {
        if (node instanceof RexLiteral literal) {
            if (literal.getTypeName() == SqlTypeName.CHAR) {
                return literal.getValueAs(String.class);
            }
        }
        // Handle CAST('author'):BIGINT — unwrap the cast to get the inner literal
        if (node instanceof RexCall cast && cast.getKind() == org.apache.calcite.sql.SqlKind.CAST) {
            if (!cast.getOperands().isEmpty()) {
                return extractFieldName(cast.getOperands().get(0));
            }
        }
        return null;
    }

    private record ItemAccess(int projectIndex, String fieldName) {}
}
