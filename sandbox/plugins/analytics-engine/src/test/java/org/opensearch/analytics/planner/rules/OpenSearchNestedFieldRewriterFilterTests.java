/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Validates the hypothesis behind the proposed upstream PPL-validator fix: if the validator
 * emitted {@code ITEM($arrayCol,'field') > literal} for a dotted nested predicate
 * ({@code where comments.score > 4}) — the same shape it already emits for dotted projections —
 * then {@link OpenSearchNestedFieldRewriter} would handle it with no expand/translation needed.
 *
 * <p>These tests build the exact RelNode tree such a validator would produce and assert the
 * rewriter injects the Correlate+Uncollect (UNNEST) shape and eliminates every ITEM ref.
 */
public class OpenSearchNestedFieldRewriterFilterTests extends BasePlannerRulesTests {

    /** blogs: $0 comments ARRAY&lt;ROW(author VARCHAR, score INT)&gt;, $1 title VARCHAR, $2 views INT. */
    private RelNode blogsScan() {
        RelDataType authorType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType scoreType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType structType = typeFactory.createStructType(List.of(authorType, scoreType), List.of("author", "score"));
        RelDataType arrayType = typeFactory.createArrayType(structType, -1);

        RelDataTypeFactory.Builder b = typeFactory.builder();
        b.add("comments", arrayType);
        b.add("title", typeFactory.createSqlType(SqlTypeName.VARCHAR));
        b.add("views", typeFactory.createSqlType(SqlTypeName.INTEGER));
        RelDataType rowType = b.build();

        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("blogs"));
        when(table.getRowType()).thenReturn(rowType);
        return stubScan(table);
    }

    /** ITEM($0, 'field') against the comments array column of the given input. */
    private RexNode itemRef(RelNode input, String field) {
        RelDataType arrayType = input.getRowType().getFieldList().get(0).getType();
        return rexBuilder.makeCall(
            SqlStdOperatorTable.ITEM,
            rexBuilder.makeInputRef(arrayType, 0),
            rexBuilder.makeLiteral(field)
        );
    }

    /**
     * Simulates "source=blogs | where comments.score &gt; 4 | fields title" as the validator
     * WOULD emit it: Project(title) over Filter(ITEM($0,'score') &gt; 4) over Scan(blogs).
     */
    public void testDottedNestedFilter_rewrittenToUnnest() {
        RelNode scan = blogsScan();
        RexNode gt4 = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            itemRef(scan, "score"),
            rexBuilder.makeLiteral(4, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode filter = LogicalFilter.create(scan, gt4);
        RelNode project = LogicalProject.create(
            filter,
            List.of(),
            List.of(rexBuilder.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1)),
            List.of("title")
        );

        RelNode result = OpenSearchNestedFieldRewriter.rewrite(project);
        String plan = RelOptUtil.toString(result);

        assertTrue("rewrite must inject Correlate:\n" + plan, plan.contains("LogicalCorrelate"));
        assertTrue("rewrite must inject Uncollect:\n" + plan, plan.contains("Uncollect"));
        assertFalse("no ITEM may survive the rewrite:\n" + plan, plan.contains("ITEM("));
        // Filter now references the appended unnested column ($4 = score), not the array.
        assertTrue("filter must compare the unnested score column:\n" + plan, plan.contains(">($4, 4)"));
        // Output row type is unchanged: just `title`.
        assertEquals(List.of("title"), result.getRowType().getFieldNames());
    }

    /** Same-child AND: ITEM($0,'score') > 4 AND ITEM($0,'author') = 'carol' — one shared unnest. */
    public void testDottedNestedFilter_compoundAnd_singleUnnest() {
        RelNode scan = blogsScan();
        RexNode gt4 = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            itemRef(scan, "score"),
            rexBuilder.makeLiteral(4, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RexNode eqCarol = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, itemRef(scan, "author"), rexBuilder.makeLiteral("carol"));
        RelNode filter = LogicalFilter.create(scan, rexBuilder.makeCall(SqlStdOperatorTable.AND, gt4, eqCarol));

        RelNode result = OpenSearchNestedFieldRewriter.rewrite(filter);
        String plan = RelOptUtil.toString(result);

        assertFalse("no ITEM may survive the rewrite:\n" + plan, plan.contains("ITEM("));
        long correlates = plan.lines().filter(l -> l.contains("LogicalCorrelate")).count();
        assertEquals("both conditions bind to ONE unnest (same-child semantics):\n" + plan, 1, correlates);
    }

    /** A flat predicate (views > 100) must pass through untouched — the rewriter is a no-op. */
    public void testFlatFilter_untouched() {
        RelNode scan = blogsScan();
        RexNode gt100 = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2),
            rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode filter = LogicalFilter.create(scan, gt100);

        RelNode result = OpenSearchNestedFieldRewriter.rewrite(filter);

        assertSame("flat filter must be returned unchanged", filter, result);
    }
}
