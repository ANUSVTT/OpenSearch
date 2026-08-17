/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.index.engine.dataformat.DocumentInput;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Translates between Lucene docId space and Parquet logical-row space for one composite segment, so a
 * nested-predicate match found in Lucene can restrict the right Parquet rows.
 *
 * <p><b>Why this exists.</b> Under OpenSearch {@code nested} mapping a single logical document is indexed
 * as a contiguous block of {@code N+1} Lucene docs ({@code N} children, then the parent/root — see
 * {@code LuceneDocumentInput}), while the Parquet primary stores exactly one row per logical document. So on
 * a nested segment {@code luceneDocId != parquetRow}. The delegated-filter contract is expressed in
 * logical-row space (the {@code [minRow,maxRow)} window is a Parquet row-group slice and the returned bitset
 * indexes logical rows), so a match found in Lucene docId space must be translated back to its logical row.
 *
 * <p><b>Two directions, both O(1).</b>
 * <ul>
 *   <li><b>row → docId</b> (clamp a row-group window to the docId range to scan): a direct index into the
 *       cached {@link #parentDocIds} array. Committed segments guarantee dense, sequential {@code __row_id__}
 *       ({@code 0,1,2,…} — enforced by {@code LuceneWriter#assertRowIdsSequential}), so array index == rowId
 *       and no search is needed.</li>
 *   <li><b>docId → row</b> (map a matched root doc to its logical row): {@code advanceExact(docId)} on the
 *       live {@code __row_id__} {@link SortedNumericDocValues}. Matched roots arrive in ascending docId order
 *       during a scan, so the forward-only doc-values cursor is O(1) amortized. The caller owns the DV
 *       instance (one per scan) and passes it in — this class caches only the immutable row→docId array.</li>
 * </ul>
 *
 * <p><b>Caching.</b> The {@link #parentDocIds} array is immutable for a segment's life, so it is built once
 * per segment and cached by the segment's {@link IndexReader.CacheKey}. A close listener drops the entry when
 * the segment is merged away — the same segment-lifetime pattern OpenSearch's {@code BitsetFilterCache} uses.
 * The block-join parent {@code BitSet} that rolls child matches up to roots is cached separately by vanilla
 * {@code BitsetFilterCache}; this class only adds the row↔docId coordinate map.
 *
 * <p><b>Flat segments.</b> A non-nested segment has no parent field and no {@code __row_id__} doc-values;
 * {@link #forLeaf} returns a pass-through translator ({@code docId == row}), so the existing flat delegation
 * path is unchanged and pays no overhead.
 */
final class RowIdTranslator {

    /** Sentinel returned by {@link #rowForDocId} when a docId has no logical row (defensive; see javadoc). */
    static final long NO_ROW = -1L;

    /**
     * Per-segment cache of the {@code parentDocIds} array, keyed by the segment's core cache key. Entries are
     * removed by a close listener when the segment is dropped, so this never grows unbounded. Value is the
     * immutable row→docId map ({@code parentDocIds[row] = root docId}); absent key on a flat segment.
     */
    private static final Map<IndexReader.CacheKey, int[]> PARENT_DOC_IDS_CACHE = new ConcurrentHashMap<>();

    private final boolean nested;
    private final int logicalRowCount;

    /** {@code parentDocIds[row]} = the root docId of logical row {@code row}. Null on a flat (pass-through) segment. */
    private final int[] parentDocIds;

    private RowIdTranslator(boolean nested, int logicalRowCount, int[] parentDocIds) {
        this.nested = nested;
        this.logicalRowCount = logicalRowCount;
        this.parentDocIds = parentDocIds;
    }

    /**
     * Returns the translator for {@code leafContext}, building the {@code parentDocIds} array on first use for
     * a nested segment and caching it for the segment's lifetime. Non-nested segments return a pass-through
     * translator without touching the cache.
     */
    static RowIdTranslator forLeaf(LeafReaderContext leafContext) throws IOException {
        LeafReader reader = leafContext.reader();
        int maxDoc = reader.maxDoc();
        String parentField = reader.getFieldInfos().getParentField();
        // __row_id__ is written as a SortedNumericDocValuesField (see LuceneDocumentInput.setRowId), so it must
        // be read via getSortedNumericDocValues; getNumericDocValues returns null for it.
        SortedNumericDocValues rowIdDV = reader.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);

        // Non-nested segment: no parent field or no __row_id__ doc-values → pass-through (docId == row).
        if (parentField == null || rowIdDV == null) {
            org.apache.logging.log4j.LogManager.getLogger(RowIdTranslator.class)
                .info("[NAM-RIT] FLAT leaf maxDoc={} parentField={} hasRowIdDV={}", maxDoc, parentField, rowIdDV != null);
            return new RowIdTranslator(false, maxDoc, null);
        }

        int[] cached = lookupOrBuild(reader, rowIdDV);
        org.apache.logging.log4j.LogManager.getLogger(RowIdTranslator.class)
            .info(
                "[NAM-RIT] NESTED leaf maxDoc={} parentField={} logicalRows={} firstParentDocIds={}",
                maxDoc,
                parentField,
                cached.length,
                java.util.Arrays.toString(java.util.Arrays.copyOf(cached, Math.min(cached.length, 12)))
            );
        return new RowIdTranslator(true, cached.length, cached);
    }

    /**
     * Returns the cached {@code parentDocIds} array for {@code reader}, building it (once) from the
     * {@code __row_id__} doc-values if absent. The array is registered with a close listener so it is evicted
     * when the segment is dropped. Falls back to an uncached build if the reader exposes no core cache helper.
     */
    private static int[] lookupOrBuild(LeafReader reader, SortedNumericDocValues rowIdDV) throws IOException {
        IndexReader.CacheHelper cacheHelper = reader.getCoreCacheHelper();
        if (cacheHelper == null) {
            return buildParentDocIds(reader.maxDoc(), rowIdDV);
        }
        IndexReader.CacheKey key = cacheHelper.getKey();
        int[] existing = PARENT_DOC_IDS_CACHE.get(key);
        if (existing != null) {
            org.apache.logging.log4j.LogManager.getLogger(RowIdTranslator.class)
                .info("[NAM-RIT] parentDocIds CACHE HIT segKey={} rows={}", System.identityHashCode(key), existing.length);
            return existing;
        }
        // Build outside the map's compute lock (the DV scan is O(maxDoc) I/O); tolerate a rare duplicate build
        // under a race by letting putIfAbsent pick a single winner. The array is value-identical either way.
        int[] built = buildParentDocIds(reader.maxDoc(), rowIdDV);
        int[] prior = PARENT_DOC_IDS_CACHE.putIfAbsent(key, built);
        if (prior != null) {
            org.apache.logging.log4j.LogManager.getLogger(RowIdTranslator.class)
                .info("[NAM-RIT] parentDocIds RACE-LOST segKey={} rows={}", System.identityHashCode(key), prior.length);
            return prior;
        }
        cacheHelper.addClosedListener(PARENT_DOC_IDS_CACHE::remove);
        org.apache.logging.log4j.LogManager.getLogger(RowIdTranslator.class)
            .info("[NAM-RIT] parentDocIds BUILD (cache miss) segKey={} rows={}", System.identityHashCode(key), built.length);
        return built;
    }

    /**
     * Scans the {@code __row_id__} doc-values (present only on root docs) once, collecting root docIds in
     * ascending docId order. Because the segment is index-sorted by {@code __row_id__}, that is also ascending
     * rowId order, so {@code parentDocIds[i]} is the root of logical row {@code i}.
     */
    private static int[] buildParentDocIds(int maxDoc, SortedNumericDocValues rowIdDV) throws IOException {
        int[] docIds = new int[maxDoc];
        int n = 0;
        for (int docId = rowIdDV.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = rowIdDV.nextDoc()) {
            docIds[n++] = docId;
        }
        if (n == docIds.length) {
            return docIds;
        }
        int[] trimmed = new int[n];
        System.arraycopy(docIds, 0, trimmed, 0, n);
        return trimmed;
    }

    /**
     * Eagerly build + cache the {@code parentDocIds} array for every nested leaf of {@code reader}, so the
     * first delegated nested query finds it warm instead of paying the O(maxDoc) {@code __row_id__} doc-values
     * scan on the request path. This mirrors vanilla's {@code IndicesBitsetFilterCache.BitSetProducerWarmer},
     * which eagerly loads the root/parent bitset on every new segment (default-on
     * {@code index.load_fixed_bitset_filters_eagerly}) — the reason vanilla's nested cold latency ≈ warm.
     * Best-effort: any per-leaf failure is swallowed (the query path rebuilds lazily as before). Intended to
     * be called from a refresh hook on a background thread, never on the query path.
     */
    static void warmNestedCaches(org.apache.lucene.index.IndexReader reader) {
        for (LeafReaderContext leaf : reader.leaves()) {
            try {
                forLeaf(leaf); // populates PARENT_DOC_IDS_CACHE for nested leaves; no-op/cheap for flat
            } catch (Exception e) {
                org.apache.logging.log4j.LogManager.getLogger(RowIdTranslator.class)
                    .debug("[NAM-RIT] warm skipped for a leaf (will build lazily): {}", e.toString());
            }
        }
    }

    boolean isNested() {
        return nested;
    }

    /** Number of logical rows in this leaf: parent count on a nested leaf, {@code maxDoc()} on a flat leaf. */
    int logicalRowCount() {
        return logicalRowCount;
    }

    /**
     * The logical row for a matched docId. Flat: identity. Nested: reads the docId's {@code __row_id__} from
     * {@code rowIdDV} (which the caller advances forward across matches). Returns {@link #NO_ROW} if the docId
     * carries no {@code __row_id__} — i.e. it is a child, which a correctly block-joined query never yields.
     */
    long rowForDocId(int docId, SortedNumericDocValues rowIdDV) throws IOException {
        if (nested == false) {
            return docId;
        }
        if (rowIdDV.advanceExact(docId)) {
            // Each root carries exactly one __row_id__ value (docValueCount == 1); nextValue reads it.
            return rowIdDV.nextValue();
        }
        return NO_ROW;
    }

    /**
     * The first Lucene docId to scan from to cover logical row {@code row} (inclusive). Flat: {@code row}.
     * Nested: children precede their root, so the block owning {@code row} starts just after the previous
     * root's docId (or at 0 for the first row).
     */
    int firstDocIdForRow(int row) {
        if (nested == false) {
            return row;
        }
        if (row <= 0) {
            return 0;
        }
        // row is dense: parentDocIds[row-1] is the previous root; this row's block starts right after it.
        return parentDocIds[row - 1] + 1;
    }

    /**
     * The exclusive Lucene docId bound to stop scanning at when covering logical rows up to
     * {@code rowExclusive}. Flat: {@code rowExclusive}. Nested: one past the root docId of the last row in
     * range (a block ends at its root, the highest docId in the block).
     */
    int docIdScanBoundForRow(int rowExclusive) {
        if (nested == false) {
            return rowExclusive;
        }
        if (rowExclusive <= 0) {
            return 0;
        }
        // Last logical row in range is rowExclusive-1; its root docId is that block's max docId.
        return parentDocIds[rowExclusive - 1] + 1;
    }
}
