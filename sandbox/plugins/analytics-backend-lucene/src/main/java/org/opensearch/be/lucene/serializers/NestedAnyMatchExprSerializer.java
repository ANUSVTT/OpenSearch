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
 * a {@link TermQueryBuilder}. That leaf may itself sit inside 1+ {@code {"nested":hop,"inner":...}}
 * wrappers — the shape a MULTI-LEVEL single-conjunct dotted path (e.g.
 * {@code products.variants.color = "red"}, with no other conjunct on the array to trigger the
 * separate child-grain-split path) produces. {@link #canServe} inspects the tree and approves only
 * an equality leaf at the root OR at the bottom of a chain of {@code "nested"} wrappers;
 * {@code OpenSearchFilterRule} calls it before ever reaching {@link #buildQueryBuilder}, so this
 * method can assume the shape it recognizes.
 *
 * <p>Builds vanilla OpenSearch's own native nested-query primitive: a {@link TermQueryBuilder} on
 * the dotted leaf field ({@code <arrayCol>.<hop1>...<hopN>.<field>}), wrapped in one
 * {@link NestedQueryBuilder} per level crossed — innermost first, e.g. for a 1-boundary chain,
 * {@code NestedQueryBuilder("products", NestedQueryBuilder("products.variants", term(...)))} —
 * matching vanilla's own construction for a multi-level {@code nested} query (each level's path is
 * the FULL dotted path up to and including that level, per OpenSearch's {@code nested} query
 * contract). {@code ScoreMode.None} throughout: this predicate is used purely for filtering, never
 * scoring.
 */
public class NestedAnyMatchExprSerializer extends AbstractQuerySerializer {

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        NestedAnyMatchExprSerializer.class
    );

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
        String arrayColPath = arrayField.getFieldName();

        // Build the full dotted leaf field path, and the list of nested-query paths to wrap, OUTERMOST
        // first: [arrayColPath, arrayColPath.hop1, arrayColPath.hop1.hop2, ...] — one NestedQueryBuilder
        // per level, each path the FULL dotted path up to that level (OpenSearch's own nested-query
        // contract for multi-level nesting).
        StringBuilder pathBuilder = new StringBuilder(arrayColPath);
        List<String> nestedPaths = new java.util.ArrayList<>();
        nestedPaths.add(pathBuilder.toString());
        for (String hop : leaf.hops()) {
            pathBuilder.append('.').append(hop);
            nestedPaths.add(pathBuilder.toString());
        }
        String leafField = pathBuilder + "." + leaf.field();

        QueryBuilder inner = new TermQueryBuilder(leafField, leaf.value());
        for (int i = nestedPaths.size() - 1; i >= 0; i--) {
            inner = new NestedQueryBuilder(nestedPaths.get(i), inner, ScoreMode.None);
        }
        LOGGER.info(
            "[NAM-SER] arrayCol=[{}] hops={} leafField=[{}] value=[{}] nestedPaths={} -> block-join depth={}",
            arrayColPath,
            leaf.hops(),
            leafField,
            leaf.value(),
            nestedPaths,
            nestedPaths.size()
        );
        return inner;
    }

    /** {@code hops} is the ordered list of intermediate nested-array field names crossed BELOW the
     *  array column, before reaching {@code field} — empty for the pre-existing single-level shape. */
    private record EqualityLeaf(List<String> hops, String field, String value) {}

    /**
     * Returns the single equality leaf this call's JSON tree describes — possibly nested inside 1+
     * {@code {"nested":hop,"inner":...}} wrappers — or {@code null} if the tree is anything else
     * (compound, arithmetic, non-string value, malformed, ...).
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
        return parseEqualityLeaf(root, new java.util.ArrayList<>());
    }

    /** Recursive helper: descends through {@code "nested"} wrappers, accumulating hop names, until it
     *  finds an equality leaf or hits a shape it doesn't recognize. */
    private static EqualityLeaf parseEqualityLeaf(JsonNode node, List<String> hopsSoFar) {
        if (!node.isObject()) {
            return null;
        }
        if (node.has("nested") && node.has("inner")) {
            JsonNode nestedField = node.get("nested");
            if (!nestedField.isTextual()) {
                return null;
            }
            List<String> hops = new java.util.ArrayList<>(hopsSoFar);
            hops.add(nestedField.asText());
            return parseEqualityLeaf(node.get("inner"), hops);
        }
        if (!"=".equals(node.path("op").asText(null))) {
            return null;
        }
        JsonNode args = node.get("args");
        if (args == null || !args.isArray() || args.size() != 2) {
            return null;
        }
        FieldAndValue fv = fieldAndLiteral(args.get(0), args.get(1));
        if (fv == null) {
            fv = fieldAndLiteral(args.get(1), args.get(0));
        }
        return fv == null ? null : new EqualityLeaf(hopsSoFar, fv.field(), fv.value());
    }

    private record FieldAndValue(String field, String value) {}

    private static FieldAndValue fieldAndLiteral(JsonNode maybeField, JsonNode maybeLiteral) {
        if (!maybeField.isObject() || !maybeField.has("field") || !maybeLiteral.isObject() || !maybeLiteral.has("lit")) {
            return null;
        }
        JsonNode fieldNode = maybeField.get("field");
        JsonNode litNode = maybeLiteral.get("lit");
        if (!fieldNode.isTextual() || !litNode.isTextual()) {
            return null;
        }
        return new FieldAndValue(fieldNode.asText(), litNode.asText());
    }
}
