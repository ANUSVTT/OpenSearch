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
import org.opensearch.analytics.planner.rules.OpenSearchNestedFieldRewriter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link NestedAnyMatchChildSerializer}, covering both the pre-existing single-level shape
 * and the multi-level chain shape added by {@code MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md}'s Component B.
 *
 * <p>{@link #testMultiLevelChain_targetsDeepestPathNotArrayColumnPath} is the dedicated regression test
 * FR-3 of that plan requires: it proves the serializer targets the DEEPEST crossed nested level's own
 * {@code _nested_path}, not the outer array column's path — getting this wrong doesn't throw, it
 * silently builds a query that matches zero children (see the class javadoc), so the test asserts the
 * exact built query shape rather than just "it doesn't error."
 */
public class NestedAnyMatchChildSerializerTests extends OpenSearchTestCase {

    private final NestedAnyMatchChildSerializer serializer = new NestedAnyMatchChildSerializer();
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

    private RexCall buildChildCall(String fieldName, String value, int clauseIdx) {
        return (RexCall) rexBuilder.makeCall(
            OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_CHILD_OP,
            rexBuilder.makeInputRef(arrayType, 0),
            rexBuilder.makeLiteral(fieldName),
            rexBuilder.makeLiteral("EQUALS"),
            rexBuilder.makeLiteral(value),
            rexBuilder.makeLiteral(Integer.toString(clauseIdx))
        );
    }

    /** Pre-existing single-level shape: field name is the bare leaf, _nested_path = the array column's own path. */
    public void testSingleLevel_targetsArrayColumnPath() {
        RexCall call = buildChildCall("author", "alice", 0);
        BoolQueryBuilder built = (BoolQueryBuilder) serializer.buildQueryBuilder(call, FIELD_STORAGE);

        TermQueryBuilder mustTerm = (TermQueryBuilder) built.must().get(0);
        assertEquals("products.author", mustTerm.fieldName());
        assertEquals("alice", mustTerm.value());

        TermQueryBuilder filterTerm = (TermQueryBuilder) built.filter().get(0);
        assertEquals("_nested_path", filterTerm.fieldName());
        assertEquals("products", filterTerm.value());
    }

    /**
     * THE REGRESSION TEST: a multi-level chain (field name "variants.color", i.e. a leaf 1 nested-array
     * boundary below the array column) MUST target "_nested_path" = "products.variants" — the DEEPEST
     * crossed level — not "products" (the array column's own path). If this assertion were reversed
     * (asserting "products"), the test would still compile and the code could still silently regress to
     * the wrong-level bug without any test catching it — this is the exact failure mode FR-3 exists for.
     */
    public void testMultiLevelChain_targetsDeepestPathNotArrayColumnPath() {
        RexCall call = buildChildCall("variants.color", "red", 0);
        BoolQueryBuilder built = (BoolQueryBuilder) serializer.buildQueryBuilder(call, FIELD_STORAGE);

        TermQueryBuilder mustTerm = (TermQueryBuilder) built.must().get(0);
        assertEquals("the leaf field itself must still be the full dotted path", "products.variants.color", mustTerm.fieldName());
        assertEquals("red", mustTerm.value());

        TermQueryBuilder filterTerm = (TermQueryBuilder) built.filter().get(0);
        assertEquals("_nested_path", filterTerm.fieldName());
        assertEquals(
            "_nested_path must target the DEEPEST crossed level (products.variants), NOT the array "
                + "column's own path (products) — targeting the wrong level doesn't error, it silently "
                + "matches zero children (a term restricted to the wrong nested scope can never match), "
                + "which is the exact bug this test exists to catch",
            "products.variants",
            filterTerm.value()
        );
    }

    /** Depth-2 chain (2 nested-array boundaries below the array column): deepest path has 2 extra segments. */
    public void testTwoLevelChain_targetsDeepestOfTwoBoundaries() {
        RexCall call = buildChildCall("variants.specs.key", "weight", 0);
        BoolQueryBuilder built = (BoolQueryBuilder) serializer.buildQueryBuilder(call, FIELD_STORAGE);

        TermQueryBuilder mustTerm = (TermQueryBuilder) built.must().get(0);
        assertEquals("products.variants.specs.key", mustTerm.fieldName());

        TermQueryBuilder filterTerm = (TermQueryBuilder) built.filter().get(0);
        assertEquals("products.variants.specs", filterTerm.value());
    }

    public void testNonEqualsOpThrows() {
        RexCall call = (RexCall) rexBuilder.makeCall(
            OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_CHILD_OP,
            rexBuilder.makeInputRef(arrayType, 0),
            rexBuilder.makeLiteral("author"),
            rexBuilder.makeLiteral("NOT_EQUALS"),
            rexBuilder.makeLiteral("alice"),
            rexBuilder.makeLiteral("0")
        );
        assertThrows(IllegalArgumentException.class, () -> serializer.buildQueryBuilder(call, FIELD_STORAGE));
    }

    private static void assertThrows(Class<? extends Throwable> expected, Runnable r) {
        try {
            r.run();
            fail("Expected " + expected.getSimpleName() + " but nothing was thrown");
        } catch (Throwable t) {
            if (!expected.isInstance(t)) {
                throw new AssertionError("Expected " + expected.getSimpleName() + " but got " + t.getClass().getSimpleName(), t);
            }
        }
    }
}
