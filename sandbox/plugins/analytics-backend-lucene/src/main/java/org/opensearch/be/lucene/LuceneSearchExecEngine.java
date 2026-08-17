/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene-side {@link SearchExecEngine}. Mirrors {@code DatafusionSearchExecEngine}'s role
 * for the Lucene backend: takes the {@link LuceneSearcherState} produced upstream by the
 * instruction handler, executes the operation, and returns an {@link EngineResultStream}
 * the framework drains into the Flight transport.
 *
 * <p>Today's only operation is the count fast path —
 * {@link org.apache.lucene.search.IndexSearcher#count(org.apache.lucene.search.Query)} —
 * exported through the Arrow C-Data interface so the result VSR has the same
 * foreign-allocation-managed buffer layout DataFusion's result stream produces. Pure-Java
 * {@code setSafe}-built VSRs don't survive Flight's {@code VectorTransfer.transferRoot};
 * see {@link LuceneResultStream} for the detailed comparison.
 *
 * <p>No deletes gate. {@code IndexSearcher.count} is self-healing: per-leaf
 * {@code Weight.count(leaf)} returns -1 on dirty leaves and falls back to full iteration —
 * correct under deletes, just slower. Even the slow case is substantially cheaper than
 * DataFusion decoding rows.
 *
 * @opensearch.internal
 */
final class LuceneSearchExecEngine implements SearchExecEngine<ShardScanExecutionContext, EngineResultStream> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneSearchExecEngine.class);

    private final LuceneSearcherState state;

    LuceneSearchExecEngine(LuceneSearcherState state) {
        this.state = state;
    }

    @Override
    public void prepare(ShardScanExecutionContext context) {
        // No preparation needed — the LuceneSearcherState was fully built by the instruction
        // handler. {@code prepare} is part of the SearchExecEngine contract for backends that
        // need to assemble plans from the context (e.g. DataFusion); Lucene has nothing to do.
    }

    @Override
    public EngineResultStream execute(ShardScanExecutionContext context) throws IOException {
        // [NESTED] count(*) must return LOGICAL-document count (== Parquet rows), not raw Lucene doc
        // count. On a nested index a logical doc is a block of N+1 Lucene docs, so a plain
        // count(MatchAll) over-counts by the children. Restrict the count to PARENT docs by AND-ing the
        // filter with a parents filter (FieldExistsQuery on the block-join parent field). On a non-nested
        // index there is no parent field, so the query is used unchanged and behaviour is identical.
        // Grep: NESTED count-fastpath.
        long countStartNanos = System.nanoTime();
        long count;
        String countPath;
        // Fast path: a pure nested block-join count() counts DISTINCT ROOT docs that have >=1 matching
        // innermost child. Instead of IndexSearcher.count() — which has no fast Weight.count() for a
        // ToParentBlockJoinQuery and walks every child block up through each of the N nesting levels via
        // Scorer/TwoPhaseIterator machinery — we iterate the innermost child postings once and map each
        // child docId up to its enclosing root in O(1) via the cached parentDocIds array (the same
        // segment-lifetime cache RowIdTranslator builds). O(matching children), no per-level roll-up,
        // independent of how many parents match. Returns -1 when the shape isn't a pure single block-join
        // (mixed/flat/MatchAll), and we fall back to the exact searcher.count() below. See project_mustang_latency #2.
        long fast = parentScopedBlockJoinCount(state.searcher(), state.filterQuery());
        if (fast >= 0) {
            count = fast;
            countPath = "parent-bitset-fastpath";
        } else {
            Query countQuery = parentScopedCountQuery(state.searcher(), state.filterQuery());
            count = state.searcher().count(countQuery);
            countPath = "searcher.count";
        }
        long countMs = (System.nanoTime() - countStartNanos) / 1_000_000L;
        LOGGER.info(
            "[NESTED] lucene-count shardId={} count_ms={} count={} path={} originalQuery={} columns={}",
            context.getShardId(),
            countMs,
            count,
            countPath,
            state.filterQuery(),
            state.outputColumnNames()
        );
        BufferAllocator allocator = context.getAllocator();
        Schema schema = buildSchema(state.outputColumnNames());
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        boolean transferred = false;
        try {
            populateBatchToCData(allocator, schema, state.outputColumnNames(), count, array, arrowSchema);
            LuceneResultStream stream = new LuceneResultStream(array, arrowSchema, allocator);
            transferred = true;
            return stream;
        } finally {
            if (transferred == false) {
                try {
                    array.close();
                } finally {
                    arrowSchema.close();
                }
            }
        }
    }

    /**
     * Returns a count query scoped to PARENT (logical) documents when the index uses nested block-join
     * storage, else the original query unchanged.
     *
     * <p>A nested index stores each logical document as a block of N+1 Lucene docs (N children + parent),
     * so raw {@code count(query)} over-counts by the children. Only parents carry the {@code __row_id__}
     * doc-value, so {@code query AND FieldExists(__row_id__)} counts exactly the logical documents — which
     * equals the Parquet primary's row count. We detect "nested index" via the Lucene parent field
     * ({@code FieldInfos.getParentField()}, set to {@code __nested_parent} by the writer); when absent the
     * index is flat and the original query is returned untouched (zero behaviour change for non-nested).
     *
     * <p><b>Pure block-join fast path.</b> When the whole filter is a single {@link
     * OpenSearchToParentBlockJoinQuery} (the shape a lone nested-equality predicate serializes to, e.g.
     * {@code where comments.replies.author = "alice" | stats count()}), the extra {@code #FieldExists(__row_id__)}
     * conjunct is <em>redundant</em>: a {@code ToParentBlockJoinQuery} already resolves each matching child
     * block up to its single parent doc, so it emits <em>only</em> parent docs. AND-ing it with the parent
     * marker changes nothing about the result, but it turns a 1-clause count into a 2-required-clause
     * {@code BooleanQuery}. That forces {@code IndexSearcher.count()} down the {@code ConjunctionScorer}
     * path — full candidate-by-candidate iteration with a redundant {@code __row_id__} doc-values advance
     * per parent — because {@code ToParentBlockJoinQuery} has no fast {@code Weight.count()} and the
     * conjunction disables the single-clause bulk path. Returning the bare block-join lets {@code count()}
     * use the block-join's own scorer directly (the same path vanilla's nested count takes), which is the
     * dominant cost of nested-eq {@code count()} at scale (see project_mustang_latency). The count is
     * identical — logical parents with >=1 matching child. We keep the wrapper for every other filter
     * shape (MatchAll, flat/keyword terms, or any query that can match child docs), where the
     * {@code __row_id__} conjunct is what prevents over-counting.
     */
    private static Query parentScopedCountQuery(IndexSearcher searcher, Query filterQuery) {
        boolean nested = searcher.getIndexReader()
            .leaves()
            .stream()
            .anyMatch(leaf -> leaf.reader().getFieldInfos().getParentField() != null);
        if (nested == false) {
            return filterQuery;
        }
        // Pure block-join: already parent-scoped, so the __row_id__ conjunct is redundant — return it
        // unwrapped to keep count() on the fast single-clause block-join path.
        if (filterQuery instanceof OpenSearchToParentBlockJoinQuery) {
            return filterQuery;
        }
        Query parents = new FieldExistsQuery(DocumentInput.ROW_ID_FIELD);
        if (filterQuery instanceof MatchAllDocsQuery) {
            return parents;
        }
        return new BooleanQuery.Builder().add(filterQuery, BooleanClause.Occur.MUST)
            .add(parents, BooleanClause.Occur.FILTER)
            .build();
    }

    /**
     * Fast parent-scoped count for a PURE nested block-join filter: counts distinct root docs that have
     * at least one matching innermost child, by iterating the child query's per-leaf scorer once and rolling
     * each child docId up to its enclosing root via the cached {@code parentDocIds} array. This avoids the
     * per-level block-join roll-up and the Scorer/TwoPhaseIterator overhead of {@code IndexSearcher.count()}.
     *
     * <p>Returns {@code -1} (caller falls back to the exact {@code searcher.count()}) unless the filter is a
     * single {@link OpenSearchToParentBlockJoinQuery} — the shape a lone nested-equality predicate serializes
     * to. That shape is a pure existence roll-up with no cross-level correlation (the serializer stacks one
     * {@code NestedQueryBuilder} per level over a single leaf term), so "distinct roots of matching innermost
     * children" is exactly the logical-document count. Any other shape (mixed/flat/MatchAll, or a query that
     * can match child docs directly) is NOT safe here and takes the exact path.
     *
     * <p><b>Correctness:</b> the returned value equals {@code searcher.count(bareBlockJoin)} for this shape —
     * a root is counted once iff its block contains >=1 child matching the innermost child query, regardless
     * of nesting depth. The child query is scored per leaf (the same child postings the block-join would
     * traverse) and each match is mapped to its root by binary/forward search into {@code parentDocIds}
     * (children precede their root and arrive in ascending docId order, so a monotonic cursor is O(1) amortized).
     */
    private static long parentScopedBlockJoinCount(IndexSearcher searcher, Query filterQuery) throws IOException {
        if ((filterQuery instanceof OpenSearchToParentBlockJoinQuery) == false) {
            return -1L;
        }
        // Score only the innermost child query, then roll each matching child straight to its enclosing
        // root with a single BitSet.nextSetBit — the same primitive Lucene's block-join scorer uses. Because
        // children precede their root and the root is the block's highest docId, nextSetBit(childDoc) is the
        // root for a match at ANY nesting depth: no per-level roll-up needed. The parent bitset is the shared
        // segment-lifetime cache (warmed at refresh); count = distinct roots with >=1 matching child.
        Query childQuery = ((OpenSearchToParentBlockJoinQuery) filterQuery).getChildQuery();
        Weight childWeight = searcher.createWeight(searcher.rewrite(childQuery), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        BitSetProducer rootParents = new CachingParentBitSetProducer(
            new FieldExistsQuery(org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME)
        );
        long total = 0;
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            BitSet parents = rootParents.getBitSet(leaf);
            Scorer scorer = childWeight.scorer(leaf);
            if (parents == null || scorer == null) {
                continue; // no roots or no matches in this leaf
            }
            DocIdSetIterator it = scorer.iterator();
            int lastRoot = -1;
            for (int childDoc = it.nextDoc(); childDoc != DocIdSetIterator.NO_MORE_DOCS; childDoc = it.nextDoc()) {
                int root = parents.nextSetBit(childDoc); // child -> enclosing root, one lookup, any depth
                if (root != DocIdSetIterator.NO_MORE_DOCS && root != lastRoot) {
                    total++;
                    lastRoot = root;
                }
            }
        }
        return total;
    }

    private static Schema buildSchema(List<String> columnNames) {
        FieldType int64Nullable = new FieldType(true, new ArrowType.Int(64, true), null);
        List<Field> fields = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            fields.add(new Field(name, int64Nullable, null));
        }
        return new Schema(fields);
    }

    /**
     * Builds a one-row scratch VSR carrying {@code count} for every column, exports it to
     * the supplied {@code array}/{@code arrowSchema} via the Arrow C-Data interface, then
     * closes the scratch VSR. Mirrors the export side of {@code DatafusionResultStream}'s
     * contract: the populated {@link ArrowArray} is what {@link LuceneResultStream}
     * re-imports into its result VSR — same call shape DataFusion uses for native batches.
     */
    private static void populateBatchToCData(
        BufferAllocator allocator,
        Schema schema,
        List<String> columnNames,
        long count,
        ArrowArray array,
        ArrowSchema arrowSchema
    ) {
        VectorSchemaRoot scratch = VectorSchemaRoot.create(schema, allocator);
        try {
            scratch.allocateNew();
            for (int i = 0; i < columnNames.size(); i++) {
                BigIntVector v = (BigIntVector) scratch.getVector(i);
                v.setSafe(0, count);
            }
            scratch.setRowCount(1);
            try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                Data.exportVectorSchemaRoot(allocator, scratch, dictProvider, array, arrowSchema);
            }
        } finally {
            scratch.close();
        }
    }
}
