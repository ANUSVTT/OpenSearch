/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.engine.dataformat.DocumentInput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link BitSetProducer} for the parent-doc filter of a nested block-join query
 * ({@code ToParentBlockJoinQuery}), with a segment-lifetime cache so the parent {@link BitSet} is built once
 * per (segment, parent-query) and reused across queries.
 *
 * <p><b>Why this exists.</b> The block-join query {@code NestedAnyMatchSerializer} builds needs a
 * {@code BitSetProducer} identifying the root/parent docs of each nested block. Vanilla search gets this from
 * the shard's {@code BitsetFilterCache}, but that cache is owned by the {@code IndexService} and is not
 * reachable from the analytics delegation context ({@code ShardScanExecutionContext} exposes no
 * {@code IndexShard}/{@code IndexService}). The obvious fallback — a plain
 * {@code org.apache.lucene.search.join.QueryBitSetProducer} — caches per segment only within a single
 * producer instance, and a fresh instance is created for every delegated query, so its cache is cold every
 * time and it never evicts entries for merged-away segments.
 *
 * <p><b>What this does instead.</b> It caches the parent {@link BitSet} in a {@code static} map keyed by the
 * segment's {@link IndexReader.CacheKey} (and the parent query), and registers a close listener so the entry
 * is dropped when the segment is merged away — the same segment-lifetime, self-evicting pattern OpenSearch's
 * {@code BitsetFilterCache} and this package's {@link RowIdTranslator} use. The result is cross-query reuse
 * without needing the shard cache, and no leak. (When the analytics SPI is later extended to expose the real
 * {@code BitsetFilterCache}, this can be replaced by delegating to it — see the deferred task.)
 */
final class CachingParentBitSetProducer implements BitSetProducer {

    /** Cache of parent bitsets, keyed by (segment core cache key, parent query). Evicted on segment close. */
    private static final Map<CacheEntryKey, BitSet> CACHE = new ConcurrentHashMap<>();

    private final Query parentQuery;

    CachingParentBitSetProducer(Query parentQuery) {
        this.parentQuery = Objects.requireNonNull(parentQuery);
    }

    /**
     * Eagerly build + cache the ROOT parent-doc {@link BitSet} for every nested segment of {@code reader}, so
     * the first delegated nested query finds it warm instead of building it on the request path. Mirrors
     * vanilla's {@code IndicesBitsetFilterCache.BitSetProducerWarmer} (why vanilla nested cold latency ≈ warm).
     *
     * <p><b>Root-grain only, by design.</b> The composite delegation works at ROOT (logical-row) grain: the
     * count fast-path maps children straight to their root via {@code parentDocIds}, and the RowSelection
     * bridge translates matches to logical rows (= root docs). So the single bitset worth pre-building is the
     * root parent filter {@code FieldExists(_primary_term)} — which {@link #computeBitSet} rewrites to
     * {@code FieldExists(__row_id__)} (the root marker on the Mustang secondary), and which the outermost
     * block-join level looks up. We deliberately do NOT warm the per-nesting-level {@code _nested_path} parent
     * bitsets: those are internal block-join roll-up machinery for intermediate depths, cheap to build lazily,
     * and not what our parent-grain delegation consumes. Best-effort; the query path still builds lazily if a
     * leaf is skipped. Call off the query path (refresh thread).
     */
    static void warmAll(IndexReader reader) {
        org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(CachingParentBitSetProducer.class);
        CachingParentBitSetProducer rootProducer = new CachingParentBitSetProducer(
            new FieldExistsQuery(org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME)
        );
        int warmed = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            try {
                // Only nested segments carry the __row_id__ root marker; skip flat leaves cheaply.
                if (leaf.reader().getFieldInfos().fieldInfo(DocumentInput.ROW_ID_FIELD) == null) {
                    continue;
                }
                rootProducer.getBitSet(leaf);
                warmed++;
            } catch (Exception e) {
                log.debug("[NAM-PARENT] warm skipped for a leaf (will build lazily): {}", e.toString());
            }
        }
        log.info("[NAM-PARENT] warmAll built {} root parent bitsets across {} leaves", warmed, reader.leaves().size());
    }

    // Value-based identity over {@code parentQuery} — mirrors vanilla's
    // {@code IndicesBitsetFilterCache.QueryWrapperBitSetProducer#equals/hashCode}. This is required for a
    // reason beyond this class's own {@link #CACHE} (which is already keyed by value via {@link CacheEntryKey}):
    // a {@code BitSetProducer} is embedded inside the {@code ToParentBlockJoinQuery} that
    // {@code NestedAnyMatchSerializer} builds, and {@code ToParentBlockJoinQuery#equals} compares its parents
    // filter by object identity. A fresh producer instance is created for every delegated query (see
    // {@code LuceneAnalyticsBackendPlugin.bitsetFilter}), so without value equality here two block-join queries
    // for the SAME nested predicate compare unequal, the shard {@code LRUQueryCache} treats each request as a
    // new query, and the block-join {@code Weight}/scorer is never reused across queries. Giving the producer
    // value equality (as vanilla does) makes the block-join query stable across requests so the query cache
    // warms — the parent bitset itself is already reused via {@link #CACHE}; this fixes reuse of the join
    // scorer built on top of it. (A/B-verified: this change does NOT cause the FilterTreeCallbacks teardown
    // crash — that is a pre-existing race, fixed separately in FilterTreeCallbacks.)
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CachingParentBitSetProducer other)) {
            return false;
        }
        return parentQuery.equals(other.parentQuery);
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode() + parentQuery.hashCode();
    }

    @Override
    public BitSet getBitSet(LeafReaderContext context) throws IOException {
        IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
        if (cacheHelper == null) {
            return computeBitSet(context); // uncacheable reader — compute directly (rare; e.g. some wrappers)
        }
        CacheEntryKey key = new CacheEntryKey(cacheHelper.getKey(), parentQuery);
        BitSet existing = CACHE.get(key);
        if (existing != null) {
            // Symmetric with the BUILD log below + RowIdTranslator, so the per-query cache-hit report can
            // observe parent-bitset HIT vs MISS per invocation.
            org.apache.logging.log4j.LogManager.getLogger(CachingParentBitSetProducer.class)
                .info("[NAM-PARENT] parentBitSet CACHE HIT segKey={} card={}", System.identityHashCode(cacheHelper.getKey()), existing.cardinality());
            return existing;
        }
        BitSet built = computeBitSet(context);
        BitSet prior = CACHE.putIfAbsent(key, built == null ? EMPTY : built);
        if (prior != null) {
            return unwrap(prior);
        }
        // Evict this segment's entries (across any cached parent queries) when the segment is dropped.
        cacheHelper.addClosedListener(coreKey -> CACHE.keySet().removeIf(k -> k.coreKey.equals(coreKey)));
        return built;
    }

    /**
     * Runs {@code parentQuery} over the leaf and materializes the matching docs into a {@link BitSet}.
     * Mirrors Lucene's own {@code QueryBitSetProducer#getBitSet}: build a searcher over the top-level context,
     * create the weight, score the given leaf, and collect its docs. Returns {@code null} when nothing matches
     * (no parent docs in this segment), matching the {@link BitSetProducer} contract.
     */
    private BitSet computeBitSet(LeafReaderContext context) throws IOException {
        IndexSearcher searcher = new IndexSearcher(ReaderUtil.getTopLevelContext(context));
        searcher.setQueryCache(null);
        // NestedQueryBuilder builds the block-join parent filter as FieldExistsQuery(_primary_term) — vanilla's
        // root-doc marker. The Mustang Lucene secondary does NOT store _primary_term (it lives in the Parquet
        // primary); its root docs are marked by the __row_id__ doc-values field instead. So rewrite the parent
        // filter to FieldExistsQuery(__row_id__), which matches exactly the root docs on this segment. Without
        // this the parent filter matches nothing → the ToParentBlockJoinQuery scorer is null → zero results.
        Query effectiveParent = rewriteRootMarkerForSecondary(parentQuery);
        Query rewritten = searcher.rewrite(effectiveParent);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer scorer = weight.scorer(context);
        org.apache.logging.log4j.LogManager.getLogger(CachingParentBitSetProducer.class)
            .info(
                "[NAM-PARENT] parentQuery=[{}] rewritten=[{}] leaf(ord={},maxDoc={}) scorer={}",
                parentQuery,
                rewritten,
                context.ord,
                context.reader().maxDoc(),
                scorer == null ? "NULL" : "present"
            );
        if (scorer == null) {
            return null;
        }
        BitSet bs = BitSet.of(scorer.iterator(), context.reader().maxDoc());
        org.apache.logging.log4j.LogManager.getLogger(CachingParentBitSetProducer.class)
            .info("[NAM-PARENT] leaf(ord={}) parent bitset cardinality={}", context.ord, bs.cardinality());
        return bs;
    }

    /**
     * If {@code q} is the vanilla nested root-doc marker {@code FieldExistsQuery(_primary_term)}, rewrite it to
     * {@code FieldExistsQuery(__row_id__)} — the field the Mustang Lucene secondary actually marks root docs
     * with ({@code _primary_term} is not stored there; it lives in the Parquet primary). Any other query is
     * returned unchanged.
     */
    private static Query rewriteRootMarkerForSecondary(Query q) {
        if (q instanceof org.apache.lucene.search.FieldExistsQuery feq
            && org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME.equals(feq.getField())) {
            return new org.apache.lucene.search.FieldExistsQuery(org.opensearch.index.engine.dataformat.DocumentInput.ROW_ID_FIELD);
        }
        return q;
    }

    private static BitSet unwrap(BitSet cached) {
        return cached == EMPTY ? null : cached;
    }

    /** Sentinel for "no matching parents in this segment" so the cache can distinguish it from "not yet built". */
    private static final BitSet EMPTY = new FixedBitSet(1);

    /** Composite cache key: a parent bitset is specific to both the segment and the parent query. */
    private static final class CacheEntryKey {
        private final IndexReader.CacheKey coreKey;
        private final Query query;

        CacheEntryKey(IndexReader.CacheKey coreKey, Query query) {
            this.coreKey = coreKey;
            this.query = query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheEntryKey other)) {
                return false;
            }
            return coreKey.equals(other.coreKey) && query.equals(other.query);
        }

        @Override
        public int hashCode() {
            return 31 * coreKey.hashCode() + query.hashCode();
        }
    }
}
