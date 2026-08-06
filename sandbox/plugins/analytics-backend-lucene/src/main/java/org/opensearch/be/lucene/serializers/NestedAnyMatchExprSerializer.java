/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
 * Serializer for {@code NESTED_ANY_MATCH_EXPR(arrayCol, jsonExprTree)} — see
 * {@code OpenSearchNestedFieldRewriter}'s javadoc on {@code NESTED_ANY_MATCH_EXPR_OP} for the wire
 * format and the two-phase capability story.
 *
 * <p>The JSON tree can describe ANY per-element predicate (compound, arithmetic, ...), but this
 * serializer — and native Lucene queries in general — can only translate a single leaf equality
 * ({@code {"op":"=","args":[{"field":F},{"lit":V}]}}, either operand order, {@code V} a string) into
 * a {@link TermQueryBuilder}. {@link #canServe} inspects the tree and approves only that shape;
 * {@code OpenSearchFilterRule} calls it before ever reaching {@link #buildQueryBuilder}, so this
 * method can assume the shape it recognizes.
 *
 * <p>Builds vanilla OpenSearch's own native nested-query primitive: a {@link TermQueryBuilder} on
 * the dotted leaf field ({@code <arrayCol>.<field>}), wrapped in a {@link NestedQueryBuilder}
 * (Lucene executes this as a {@code ToParentBlockJoinQuery}, matching parent docs with ANY child
 * having that field equal to the value — the same semantics the per-element JSON-tree evaluation
 * produces natively in DataFusion for this single-leaf shape). {@code ScoreMode.None}: this
 * predicate is used purely for filtering, never scoring.
 */
public class NestedAnyMatchExprSerializer extends AbstractQuerySerializer {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Override
    public boolean canServe(RexCall call, List<FieldStorageInfo> fieldStorage) {
        return parseEqualityLeaf(call) != null;
    }

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        EqualityLeaf leaf = parseEqualityLeaf(call);
        if (leaf == null) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH_EXPR: unsupported expr tree for Lucene delegation");
        }
        List<RexNode> operands = call.getOperands();
        if (!(operands.get(0) instanceof RexInputRef arrayColRef)) {
            throw new IllegalArgumentException("NESTED_ANY_MATCH_EXPR's 1st operand must be the array column, got " + operands.get(0));
        }
        FieldStorageInfo arrayField = FieldStorageInfo.resolve(fieldStorage, arrayColRef.getIndex());
        String nestedPath = arrayField.getFieldName();
        String leafField = nestedPath + "." + leaf.field();

        QueryBuilder child = new TermQueryBuilder(leafField, leaf.value());
        return new NestedQueryBuilder(nestedPath, child, ScoreMode.None);
    }

    private record EqualityLeaf(String field, String value) {}

    /**
     * Returns the single equality leaf this call's JSON tree describes, or {@code null} if the
     * tree is anything else (compound, arithmetic, non-string value, malformed, ...).
     */
    private static EqualityLeaf parseEqualityLeaf(RexCall call) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2 || !(operands.get(1) instanceof RexLiteral jsonLit)) {
            return null;
        }
        String json = jsonLit.getValueAs(String.class);
        if (json == null) {
            return null;
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (Exception e) {
            return null;
        }
        if (!root.isObject() || !"=".equals(root.path("op").asText(null))) {
            return null;
        }
        JsonNode args = root.get("args");
        if (args == null || !args.isArray() || args.size() != 2) {
            return null;
        }
        EqualityLeaf leaf = fieldAndLiteral(args.get(0), args.get(1));
        return leaf != null ? leaf : fieldAndLiteral(args.get(1), args.get(0));
    }

    private static EqualityLeaf fieldAndLiteral(JsonNode maybeField, JsonNode maybeLiteral) {
        if (!maybeField.isObject() || !maybeField.has("field") || !maybeLiteral.isObject() || !maybeLiteral.has("lit")) {
            return null;
        }
        JsonNode fieldNode = maybeField.get("field");
        JsonNode litNode = maybeLiteral.get("lit");
        if (!fieldNode.isTextual() || !litNode.isTextual()) {
            return null;
        }
        return new EqualityLeaf(fieldNode.asText(), litNode.asText());
    }
}
