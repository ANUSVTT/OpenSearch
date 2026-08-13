/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PHASE 0 GATING TEST for {@code MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md}. This is exploratory/empirical
 * research, not a permanent regression test — its job is to answer, with certainty, whether the
 * "Lucene child-doc ordering survives a segment merge" assumption (the design's central load-bearing
 * claim) actually holds for THIS engine's real merge machinery, at depth &ge;2, and whether it ALSO
 * holds at depth 1 when combined with a sort-permutation flush (a pre-existing, currently-untested
 * combination this test discovered while investigating).
 *
 * <p>See {@code MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md} &sect;2.6/&sect;3.1 for why this must run and be
 * classified BEFORE any of that plan's production code (Components A-D) is written.
 */
public class LuceneNestedBlockOrderPhase0Tests extends LucenePluginBaseTests {

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(
        LuceneNestedBlockOrderPhase0Tests.class
    );

    private LuceneDataFormat dataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataFormat = new LuceneDataFormat();
    }

    /**
     * Baseline (no sort permutation, no merge beyond the writer's own per-generation
     * {@code forceMerge(1,true)} on flush): ingest depth-2 nested docs (root &rarr; level1 &rarr;
     * level2, i.e. 2 array boundaries), and verify {@code _nested_path} postings order for the
     * deepest path, within each root's block, matches insertion/parse order — the ordering the
     * design's chained-offset scheme depends on. This exercises the SAME per-generation
     * {@code forceMerge(1,true)} every production flush already performs (see
     * {@code LuceneWriter.flush}, "Common path: forceMerge to 1 segment"), so it is not a merge-free
     * baseline — it already includes one real Lucene-triggered segment rewrite.
     */
    public void testDepth2NestedOrderSurvivesPerGenerationForceMerge() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType leafField = mockKeywordField("leaf");

        // 3 roots, each with a variable number of level-1 elements, each with a variable number
        // of level-2 ("deep") elements — mirrors products -> variants -> (leaf) shape at depth 2.
        int[][] shape = { { 2, 1 }, { 0, 3 }, { 1 } }; // root0: L1[0] has 2 deep, L1[1] has 1 deep; root1: L1[0] has 0, L1[1] has 3; root2: L1[0] has 1
        List<String> expectedLeafValuesInParseOrder = new ArrayList<>();

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            int rowId = 0;
            int globalCounter = 0;
            for (int[] root : shape) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                for (int deepCount : root) {
                    input.startNestedChild("level1");
                    for (int d = 0; d < deepCount; d++) {
                        input.startNestedChild("level1.level2");
                        String value = "v" + (globalCounter++);
                        input.addField(leafField, value);
                        expectedLeafValuesInParseOrder.add(value);
                        input.endNestedChild(); // level1.level2
                    }
                    input.endNestedChild(); // level1
                }
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, rowId++);
                writer.addDoc(input);
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                assertThat_(reader.leaves().size(), 1, "expected exactly 1 segment after per-generation forceMerge");
                LeafReader leaf = reader.leaves().get(0).reader();

                List<Integer> level2DocIdsAscending = collectDocIdsForPath(leaf, "level1.level2");
                assertThat_(
                    level2DocIdsAscending.size(),
                    expectedLeafValuesInParseOrder.size(),
                    "expected one level1.level2 child doc per leaf value"
                );

                // The actual invariant under test: reading the leaf field off each level1.level2
                // child doc, in ASCENDING DOCID order, must reproduce parse order exactly. This is
                // the Lucene-side half of "position implies parentage" — the Arrow/Parquet side
                // (VSRManager) is assumed (per research) to preserve the same parse order
                // independently; this test only verifies Lucene's own side of that agreement.
                for (int i = 0; i < level2DocIdsAscending.size(); i++) {
                    int docId = level2DocIdsAscending.get(i);
                    String actual = readKeyword(leaf, "leaf", docId);
                    assertThat_(actual, expectedLeafValuesInParseOrder.get(i), "leaf value order mismatch at ascending position " + i);
                }
            }
        }
    }

    /**
     * The gating question the design doc raised explicitly: does the SAME order-preservation
     * invariant survive when a sort-permutation flush (the composite engine's real cross-format
     * row-sync mechanism, {@code FlushInput} carrying a {@code RowIdMapping}) fires with nested
     * child docs present? Unlike the baseline test above, this exercises
     * {@code LuceneWriter.configureSortedMerge}/{@code ReorderingMergePolicy}/{@code ReorderingOneMerge}
     * — the path this plan's &sect;2.6 anticipated as the real risk.
     *
     * <p>Deliberately adversarial, not a weak spot-check: 5 roots with ASYMMETRIC child counts
     * (0, 1, 2, 3, 1), a full reverse permutation (not a 2-element swap, so every root moves a
     * different distance), and EXACT per-position assertions on both which root's children land
     * where AND their relative order within that root — not just "matches one of two plausible
     * groupings."
     *
     * <p><b>{@code @Ignore} — documents a CONFIRMED pre-existing bug, not a flaky/pending test.</b>
     * This fails today: when a composite index has BOTH nested fields AND a Parquet-sort-on-close
     * permutation flush ({@code FlushInput} carrying a {@code RowIdMapping}), the row-ID-rewrite
     * codec ({@code LuceneWriterDocValuesFormat.SequentialRowIdDocValues}) stamps EVERY doc in the
     * merged segment — including nested children — with a sequential {@code __row_id__}, violating
     * the "only roots carry {@code __row_id__}" invariant {@code NestedChildOrdinalMap} depends on.
     * Not live in any index tested so far (none configure {@code index.sort.field}) — see
     * {@code MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md}'s Phase 0 discussion for the full analysis and
     * why it's tracked separately rather than blocking that plan (the plan's own Components A-D all
     * go through the UNAFFECTED background cross-generation merge path, verified by the companion
     * {@code LuceneMergerNestedPhase0Tests}, not this one). Un-{@code @Ignore} once that codec bug
     * is fixed.
     */
    @org.junit.Ignore(
        "Pre-existing bug: row-ID-rewrite codec stamps nested children with a spurious __row_id__ "
            + "under sort-on-close + nested fields — see this method's javadoc. Not related to "
            + "MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md's Components A-D, which use a different, "
            + "unaffected merge path."
    )
    public void testDepth1NestedOrderUnderSortPermutationReorder() throws IOException {
        Path baseDir = createTempDir();
        MappedFieldType leafField = mockKeywordField("leaf");

        int numRoots = 5;
        int[] childCountPerRoot = { 0, 1, 2, 3, 1 };

        // Full reverse permutation: old row i -> new row (numRoots-1-i). Every root moves a
        // different distance (unlike a simple 2-element swap), so a docID/rowId conflation bug
        // would scatter children unpredictably rather than happening to still look grouped.
        long[] oldRowIds = new long[numRoots];
        long[] newRowIds = new long[numRoots];
        for (int i = 0; i < numRoots; i++) {
            oldRowIds[i] = i;
            newRowIds[i] = numRoots - 1 - i;
        }
        FlushInput sortedFlushInput = new FlushInput(buildMapping(oldRowIds, newRowIds));

        try (
            LuceneWriter writer = new LuceneWriter(
                1L,
                0L,
                dataFormat,
                baseDir,
                null,
                Codec.getDefault(),
                null,
                ConcurrentHashMap.newKeySet(),
                new LuceneShardStatsTracker()
            )
        ) {
            // expectedByOldRoot[r] = the ordered list of leaf values root r's children were
            // written with, in parse order — the ground truth this test will look for after
            // reorder, indexed by where root r's NEW row id says it should land.
            List<List<String>> expectedByOldRoot = new ArrayList<>();
            for (int r = 0; r < numRoots; r++) {
                LuceneDocumentInput input = new LuceneDocumentInput();
                List<String> values = new ArrayList<>();
                for (int c = 0; c < childCountPerRoot[r]; c++) {
                    String v = "r" + r + "c" + c;
                    input.startNestedChild("children");
                    input.addField(leafField, v);
                    input.endNestedChild();
                    values.add(v);
                }
                input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, r);
                writer.addDoc(input);
                expectedByOldRoot.add(values);
            }

            FileInfos fileInfos;
            boolean threw = false;
            String thrownMessage = null;
            try {
                fileInfos = writer.flush(sortedFlushInput);
            } catch (RuntimeException | AssertionError e) {
                threw = true;
                thrownMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
                fileInfos = null;
            }

            if (threw) {
                // A loud failure here is actually the SAFE outcome for this plan's purposes (fails
                // closed, not silently wrong) — but it is a signal the sort-permutation flush path
                // does not support nested children at all today, independent of depth. Record via
                // fail() so it shows up clearly in the test report rather than being swallowed.
                fail(
                    "Sort-permutation flush with nested children present threw ("
                        + thrownMessage
                        + "). This means the composite engine's row-sync mechanism cannot be combined "
                        + "with nested docs at all today — a PRE-EXISTING gap independent of multi-level "
                        + "depth. Phase 0 of MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md classifies this as "
                        + "case (c): a new, different problem — STOP and re-open root-cause analysis "
                        + "before proceeding with that plan."
                );
                return;
            }

            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();
            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();

                // DIAGNOSTIC DUMP (temporary, for Phase 0 investigation): full per-docId layout —
                // which docs are roots (with their __row_id__) vs children (with their _nested_path
                // and leaf value) — printed unconditionally so the raw physical layout is visible in
                // the test report regardless of whether the assertions below pass or fail.
                dumpDocLayout(leaf, leaf.maxDoc());

                // Expected flattened value sequence AFTER reorder: roots visited in ASCENDING new-row
                // order, each contributing its own values in original parse order. new row for old
                // root r is (numRoots-1-r), so old root (numRoots-1) is first, old root 0 is last.
                List<String> expectedFlattened = new ArrayList<>();
                for (int newRow = 0; newRow < numRoots; newRow++) {
                    int oldRoot = numRoots - 1 - newRow;
                    expectedFlattened.addAll(expectedByOldRoot.get(oldRoot));
                }

                List<Integer> childDocIds = collectDocIdsForPath(leaf, "children");
                List<String> actualFlattened = new ArrayList<>();
                for (int docId : childDocIds) {
                    actualFlattened.add(readKeyword(leaf, "leaf", docId));
                }

                assertThat_(
                    actualFlattened,
                    expectedFlattened,
                    "Sort-permutation flush with nested children did NOT throw, but produced a child-doc "
                        + "value sequence that doesn't exactly match the expected post-reorder grouping+order. "
                        + "This is the SILENT WRONG RESULT case the design doc's Phase 0 exists to catch — "
                        + "expected="
                        + expectedFlattened
                        + " actual="
                        + actualFlattened
                );

                // Also verify __row_id__ on the ROOT docs themselves landed correctly (0..numRoots-1
                // in ascending docId order after remap) — the pre-existing single-level invariant this
                // test also exercises as a sanity check that the reorder machinery itself ran at all.
                SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
                assertNotNull("row id doc values should exist on root docs", rowIdDV);
                List<Long> rootRowIdsInDocOrder = new ArrayList<>();
                for (int d = rowIdDV.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = rowIdDV.nextDoc()) {
                    rootRowIdsInDocOrder.add(rowIdDV.nextValue());
                }
                List<Long> expectedRootRowIds = new ArrayList<>();
                for (int i = 0; i < numRoots; i++) {
                    expectedRootRowIds.add((long) i);
                }
                assertThat_(rootRowIdsInDocOrder, expectedRootRowIds, "root __row_id__ values should be sequential 0..N-1 post-reorder");
            }
        }
    }

    // --- helpers ---

    private void dumpDocLayout(LeafReader leaf, int maxDoc) throws IOException {
        String[] nestedPathPerDoc = new String[maxDoc];
        Terms terms = leaf.terms(NestedPathFieldMapper.NAME);
        if (terms != null) {
            TermsEnum te = terms.iterator();
            BytesRef term;
            while ((term = te.next()) != null) {
                String path = term.utf8ToString();
                PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
                for (int d = pe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = pe.nextDoc()) {
                    nestedPathPerDoc[d] = path;
                }
            }
        }
        String[] leafValuePerDoc = new String[maxDoc];
        Terms leafTerms = leaf.terms("leaf");
        if (leafTerms != null) {
            TermsEnum te = leafTerms.iterator();
            BytesRef term;
            while ((term = te.next()) != null) {
                String value = term.utf8ToString();
                PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
                for (int d = pe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = pe.nextDoc()) {
                    leafValuePerDoc[d] = value;
                }
            }
        }
        long[] rowIdPerDoc = new long[maxDoc];
        java.util.Arrays.fill(rowIdPerDoc, -1L);
        SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
        if (rowIdDV != null) {
            for (int d = rowIdDV.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = rowIdDV.nextDoc()) {
                rowIdPerDoc[d] = rowIdDV.nextValue();
            }
        }
        StringBuilder sb = new StringBuilder("PHASE0 DIAGNOSTIC DOC LAYOUT (docId -> role):\n");
        for (int d = 0; d < maxDoc; d++) {
            if (rowIdPerDoc[d] >= 0) {
                sb.append("  docId=").append(d).append(" ROOT __row_id__=").append(rowIdPerDoc[d]).append('\n');
            } else {
                sb.append("  docId=")
                    .append(d)
                    .append(" CHILD _nested_path=")
                    .append(nestedPathPerDoc[d])
                    .append(" leaf=")
                    .append(leafValuePerDoc[d])
                    .append('\n');
            }
        }
        logger.info(sb.toString());
    }

    private static List<Integer> collectDocIdsForPath(LeafReader leaf, String nestedPath) throws IOException {
        List<Integer> docIds = new ArrayList<>();
        Terms terms = leaf.terms(NestedPathFieldMapper.NAME);
        if (terms == null) {
            return docIds;
        }
        TermsEnum te = terms.iterator();
        BytesRef target = new BytesRef(nestedPath);
        if (te.seekExact(target) == false) {
            return docIds;
        }
        PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
        for (int d = pe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = pe.nextDoc()) {
            docIds.add(d);
        }
        return docIds;
    }

    private static String readKeyword(LeafReader leaf, String field, int docId) throws IOException {
        // Keyword fields here are indexed (postings), not stored/doc-valued, per mockKeywordField's
        // shape (DocValuesType.NONE-equivalent — see LucenePluginBaseTests). Reconstruct the value by
        // scanning the field's term dictionary for the term whose postings include docId. Fine for
        // this small, exploratory test; production code would never do this.
        Terms terms = leaf.terms(field);
        if (terms == null) {
            return null;
        }
        TermsEnum te = terms.iterator();
        BytesRef term;
        while ((term = te.next()) != null) {
            PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
            for (int d = pe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = pe.nextDoc()) {
                if (d == docId) {
                    return term.utf8ToString();
                }
            }
        }
        return null;
    }

    private static PackedRowIdMapping buildMapping(long[] oldRowIds, long[] newRowIds) {
        int numDocs = oldRowIds.length;
        long[] oldToNew = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            oldToNew[i] = i;
        }
        for (int i = 0; i < oldRowIds.length; i++) {
            oldToNew[(int) oldRowIds[i]] = newRowIds[i];
        }
        return new PackedRowIdMapping(oldToNew, true);
    }

    private void assertThat_(Object actual, Object expected, String message) {
        assertEquals(message, expected, actual);
    }
}
