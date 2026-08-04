/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;

/**
 * Serializer for {@code NESTED_ANY_MATCH(arrayCol, field, op, value)} — the flat-equality fast
 * path {@code OpenSearchNestedFieldRewriter} emits for a standalone {@code comments.author =
 * "alice"}-style predicate (see that class's javadoc on {@code NESTED_ANY_MATCH_OP} and {@code
 * tryDirectEqualityRewrite}). Only the {@code EQUALS} operator reaches this serializer — the
 * rewriter never emits any other {@code op} value for this function.
 *
 * <p>Builds vanilla OpenSearch's own native nested-query primitive: a {@link TermQueryBuilder}
 * on the dotted leaf field ({@code <arrayCol>.<field>}), wrapped in a {@link NestedQueryBuilder}
 * (which Lucene executes as a {@code ToParentBlockJoinQuery}, matching parent docs that have ANY
 * child with that field equal to the value — the same semantics {@code NESTED_ANY_MATCH_EXPR}'s
 * per-element evaluation produces natively in DataFusion). {@code ScoreMode.None}: this predicate
 * is used purely for filtering, never scoring.
 *
 * <p>Expected RexCall shape: {@code NESTED_ANY_MATCH($arrayCol, 'field', 'EQUALS', 'value')} —
 * exactly 4 operands: an array-column {@link RexInputRef} and three string {@link RexLiteral}s.
 */
public class NestedAnyMatchSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 4) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH expects 4 operands, got " + operands.size());
        }
        if (!(operands.get(0) instanceof RexInputRef arrayColRef)) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH's 1st operand must be the array column, got " + operands.get(0));
        }
        String fieldName = literalString(operands.get(1), "field");
        String op = literalString(operands.get(2), "op");
        String value = literalString(operands.get(3), "value");
        if (!"EQUALS".equals(op)) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH performance-delegation only supports op=EQUALS, got " + op);
        }

        FieldStorageInfo arrayField = FieldStorageInfo.resolve(fieldStorage, arrayColRef.getIndex());
        String nestedPath = arrayField.getFieldName();
        String leafField = nestedPath + "." + fieldName;

        QueryBuilder child = new TermQueryBuilder(leafField, value);
        return new NestedQueryBuilder(nestedPath, child, ScoreMode.None);
    }

    private static String literalString(RexNode node, String operandName) {
        if (!(node instanceof RexLiteral lit)) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH's '" + operandName + "' operand must be a literal, got " + node);
        }
        String value = lit.getValueAs(String.class);
        if (value == null) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH's '" + operandName + "' operand must be a non-null string literal");
        }
        return value;
    }
}
