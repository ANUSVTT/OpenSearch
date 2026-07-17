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
