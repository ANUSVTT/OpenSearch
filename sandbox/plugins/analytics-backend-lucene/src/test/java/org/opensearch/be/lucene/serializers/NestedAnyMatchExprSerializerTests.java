/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link NestedAnyMatchExprSerializer}, covering both the pre-existing single-level equality
 * leaf shape and the multi-level {@code {"nested":...,"inner":...}}-wrapped leaf shape — the case a
 * bare (non-compound) multi-level dotted equality predicate produces (e.g.
 * {@code products.variants.color = "red"} with no other conjunct on the array, so the child-grain
 * split path never engages and this is the ONLY route to Lucene for that shape).
 */
public class NestedAnyMatchExprSerializerTests extends OpenSearchTestCase {

    private final NestedAnyMatchExprSerializer serializer = new NestedAnyMatchExprSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType arrayType;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("products", "nested", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        arrayType = typeFactory.createSqlType(SqlTypeName.ANY);
    }

    private RexCall buildExprCall(String json) {
        return (RexCall) rexBuilder.makeCall(
            org.opensearch.analytics.planner.rules.OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_EXPR_OP,
            rexBuilder.makeInputRef(arrayType, 0),
            rexBuilder.makeLiteral(json)
        );
    }

    public void testSingleLevelEqualityLeaf_canServeAndBuildsFlatNestedQuery() {
        RexCall call = buildExprCall("{\"op\":\"=\",\"args\":[{\"field\":\"author\"},{\"lit\":\"alice\"}]}");
        assertTrue(serializer.canServe(call, FIELD_STORAGE));

        NestedQueryBuilder built = (NestedQueryBuilder) serializer.buildQueryBuilder(call, FIELD_STORAGE);
        assertEquals("products", built.path());
        TermQueryBuilder term = (TermQueryBuilder) built.query();
        assertEquals("products.author", term.fieldName());
        assertEquals("alice", term.value());
    }

    /**
     * THE REGRESSION TEST for the newly-added multi-level support: a bare single-conjunct
     * {@code products.variants.color = "red"} predicate produces
     * {@code {"nested":"variants","inner":{"op":"=",...}}} — this MUST now be recognized by
     * canServe and built as NestedQueryBuilder("products", NestedQueryBuilder("products.variants",
     * term("products.variants.color","red"))), matching vanilla's own nested-of-nested query
     * construction. Before this fix, canServe rejected any tree with a "nested" key at the root,
     * so this shape was silently DataFusion-only even though a compound version of the exact same
     * leaf (AND-ed with a range clause) was already Lucene-viable via the separate child-grain path.
     */
    public void testOneLevelNestedWrapper_canServeAndBuildsNestedOfNestedQuery() {
        RexCall call = buildExprCall("{\"nested\":\"variants\",\"inner\":{\"op\":\"=\",\"args\":[{\"field\":\"color\"},{\"lit\":\"red\"}]}}");
        assertTrue("a leaf wrapped in one nested level must be servable", serializer.canServe(call, FIELD_STORAGE));

        NestedQueryBuilder outer = (NestedQueryBuilder) serializer.buildQueryBuilder(call, FIELD_STORAGE);
        assertEquals("products", outer.path());
        NestedQueryBuilder inner = (NestedQueryBuilder) outer.query();
        assertEquals("products.variants", inner.path());
        TermQueryBuilder term = (TermQueryBuilder) inner.query();
        assertEquals("products.variants.color", term.fieldName());
        assertEquals("red", term.value());
    }

    /** Depth-2 chain (2 nested-array boundaries below the array column): 3 nested wraps total. */
    public void testTwoLevelNestedWrapper_buildsThreeNestedLevels() {
        RexCall call = buildExprCall(
            "{\"nested\":\"variants\",\"inner\":{\"nested\":\"specs\",\"inner\":"
                + "{\"op\":\"=\",\"args\":[{\"field\":\"key\"},{\"lit\":\"weight\"}]}}}"
        );
        assertTrue(serializer.canServe(call, FIELD_STORAGE));

        NestedQueryBuilder level0 = (NestedQueryBuilder) serializer.buildQueryBuilder(call, FIELD_STORAGE);
        assertEquals("products", level0.path());
        NestedQueryBuilder level1 = (NestedQueryBuilder) level0.query();
        assertEquals("products.variants", level1.path());
        NestedQueryBuilder level2 = (NestedQueryBuilder) level1.query();
        assertEquals("products.variants.specs", level2.path());
        TermQueryBuilder term = (TermQueryBuilder) level2.query();
        assertEquals("products.variants.specs.key", term.fieldName());
        assertEquals("weight", term.value());
    }

    public void testCompoundTree_cannotServe() {
        RexCall call = buildExprCall(
            "{\"op\":\"AND\",\"args\":[{\"op\":\"=\",\"args\":[{\"field\":\"author\"},{\"lit\":\"alice\"}]},"
                + "{\"op\":\">\",\"args\":[{\"field\":\"score\"},{\"lit\":50}]}]}"
        );
        assertFalse(serializer.canServe(call, FIELD_STORAGE));
    }

    public void testNestedWrapperAroundCompound_cannotServe() {
        // A {"nested"} wrapper around a COMPOUND inner tree (not a single equality leaf) — must still
        // decline, e.g. this is what a multi-level compound predicate looks like before child-grain
        // split replaces one leaf with a {"lucene"} marker; the residual whole tree is never Lucene-servable.
        RexCall call = buildExprCall(
            "{\"nested\":\"variants\",\"inner\":{\"op\":\"AND\",\"args\":["
                + "{\"op\":\"=\",\"args\":[{\"field\":\"color\"},{\"lit\":\"red\"}]},"
                + "{\"op\":\">\",\"args\":[{\"field\":\"price\"},{\"lit\":100}]}]}}"
        );
        assertFalse(serializer.canServe(call, FIELD_STORAGE));
    }

    public void testNonEqualityLeaf_cannotServe() {
        RexCall call = buildExprCall("{\"nested\":\"variants\",\"inner\":{\"op\":\">\",\"args\":[{\"field\":\"price\"},{\"lit\":100}]}}");
        assertFalse(serializer.canServe(call, FIELD_STORAGE));
    }
}
