/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.be.lucene.merge.LuceneMerger;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * PHASE 0 GATING TEST (companion to {@code LuceneNestedBlockOrderPhase0Tests}) for
 * {@code MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md}. Exercises the BACKGROUND CROSS-GENERATION merge path
 * ({@link LuceneMerger} + {@code RowIdRemappingOneMerge}) with nested document BLOCKS present — the
 * merge path that actually runs in production without requiring {@code index.sort.field} (unlike the
 * sort-permutation-flush path covered separately, which surfaced a real pre-existing bug for that
 * narrower combination — see the companion test class and the conversation this investigation is part
 * of). This is the path every composite index with Lucene as a secondary format goes through as soon as
 * more than one writer generation needs consolidating, independent of any user-configured sort.
 */
public class LuceneMergerNestedPhase0Tests extends OpenSearchTestCase {

    private static final String ROW_ID_FIELD = DocumentInput.ROW_ID_FIELD;

    private MergeIndexWriter writer;
    private Directory directory;
    private Path dataPath;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = createTempDir();
        directory = NIOFSDirectory.open(dataPath);
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setIndexSort(new Sort(new SortedNumericSortField(ROW_ID_FIELD, SortField.Type.LONG)));
        // Required for document-block (addDocuments) support with index sorting — mirrors
        // LuceneCommitter's production config for the secondary-format case.
        iwc.setParentField(LuceneWriter.NESTED_PARENT_FIELD);
        writer = new MergeIndexWriter(directory, iwc);
    }

    @Override
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }

    /**
     * gen=1: 2 root docs, each with nested "children" (asymmetric counts: 2 and 1).
     * gen=2: 2 root docs, each with nested "children" (asymmetric counts: 0 and 3).
     * A row-ID mapping INTERLEAVES the two generations (not just concatenates them), forcing a real
     * reorder across generation boundaries — the same kind of interleaving
     * {@code testMergeWithRowIdMappingRemapsRowIds} already exercises for the no-nesting case.
     *
     * <p>After merge, verify per root: (a) the root doc's own {@code __row_id__} landed at its mapped
     * position, AND (b) that root's children are still findable via {@code _nested_path} postings and,
     * critically, do NOT carry a spurious {@code __row_id__} value of their own (the exact defect the
     * sort-permutation-flush companion test found in the OTHER merge path) — the child's identity is
     * established purely by its physical adjacency to its root in the block, not by any row-id field on
     * the child itself.
     */
    public void testMergeInterleavesNestedBlocksAcrossGenerations() throws IOException {
        // gen=1: root A (rowId=0, children a0,a1), root B (rowId=1, children b0)
        writeNestedSegment(writer, 1L, List.of(List.of("a0", "a1"), List.of("b0")));
        // gen=2: root C (rowId=0, no children), root D (rowId=1, children d0,d1,d2)
        writeNestedSegment(writer, 2L, List.of(List.of(), List.of("d0", "d1", "d2")));
        writer.commit();

        // Interleave: gen=1 root A -> new row 0, gen=2 root C -> new row 1,
        //             gen=1 root B -> new row 2, gen=2 root D -> new row 3.
        long[] mappingArray = new long[] { 0, 2, 1, 3 };
        Map<Long, Integer> genOffsets = Map.of(1L, 0, 2L, 2);
        Map<Long, Integer> genSizes = Map.of(1L, 2, 2L, 2);
        RowIdMapping rowIdMapping = new PackedRowIdMapping(mappingArray, genOffsets, genSizes);

        LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), dataPath, new LuceneShardStatsTracker());
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);
        MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(rowIdMapping).newWriterGeneration(10L).build();

        MergeResult result;
        boolean threw = false;
        String thrownMessage = null;
        try {
            result = merger.merge(input);
        } catch (RuntimeException | AssertionError | IOException e) {
            threw = true;
            thrownMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            result = null;
        }

        if (threw) {
            fail(
                "Cross-generation merge with nested document blocks present threw ("
                    + thrownMessage
                    + "). This means the background merge path (LuceneMerger) cannot be combined with "
                    + "nested docs at all today — a PRE-EXISTING gap independent of multi-level depth, and "
                    + "arguably MORE impactful than the sort-permutation-flush finding since this path runs "
                    + "unconditionally whenever multiple writer generations need consolidating. Per "
                    + "MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md's Phase 0 classification, this is case (c) — "
                    + "STOP and re-open root-cause analysis before proceeding with that plan."
            );
            return;
        }

        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            LeafReaderContext mergedLeaf = null;
            for (LeafReaderContext ctx : reader.leaves()) {
                if (mergedLeaf == null || ctx.reader().maxDoc() > mergedLeaf.reader().maxDoc()) {
                    mergedLeaf = ctx;
                }
            }
            assertNotNull("Should have at least one leaf", mergedLeaf);
            var leaf = mergedLeaf.reader();

            dumpDocLayout(leaf, leaf.maxDoc());

            // Root row IDs: must be exactly 0,1,2,3 across the 4 root docs, in ascending docId order
            // (post-remap sequential rewrite happens on THIS path too, per configureSortedMerge's
            // codec.enableRowIdRewrite() — but unlike the sort-permutation-flush path, THIS path's
            // row-id rewrite is scoped correctly if the invariant holds).
            SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(ROW_ID_FIELD);
            assertNotNull("__row_id__ doc values should exist", rowIdDV);
            List<Long> rootRowIdsInDocOrder = new ArrayList<>();
            List<Integer> rootDocIdsInOrder = new ArrayList<>();
            for (int d = rowIdDV.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = rowIdDV.nextDoc()) {
                rootRowIdsInDocOrder.add(rowIdDV.nextValue());
                rootDocIdsInOrder.add(d);
            }

            // THE CORE ASSERTION: exactly 4 docs carry a __row_id__ (the 4 roots) — if children were
            // ALSO stamped (the exact defect found in the other merge path), this count would be 8
            // (4 roots + 4 children total: 2+1+0+3=6 children, so 4 root+6 child=10 total docs, but
            // only 4 should have __row_id__).
            assertEquals(
                "Exactly 4 root docs should carry __row_id__ doc values — if this is higher, children "
                    + "are being spuriously stamped with row IDs on this merge path too (the same class of "
                    + "defect found on the sort-permutation-flush path)",
                4,
                rootRowIdsInDocOrder.size()
            );
            assertEquals("root __row_id__ values should be sequential 0..3 in ascending docId order", List.of(0L, 1L, 2L, 3L), rootRowIdsInDocOrder);

            // Total doc count sanity: 4 roots + (2+1+0+3)=6 children = 10.
            assertEquals("total doc count (roots + children)", 10, leaf.maxDoc());

            // _nested_path postings for "children" should cover exactly the 6 non-root docs.
            List<Integer> childDocIds = collectDocIdsForPath(leaf, "children");
            assertEquals("child doc count for path 'children'", 6, childDocIds.size());
        }
    }

    /**
     * DEPTH-2 variant of the test above — this is the depth the design plan's Phase 0 actually needs
     * validated (a single nested level, as tested above, is already covered by the EXISTING shipped
     * child-grain-split feature; this plan's whole premise is about N&ge;2). Structure: root &rarr;
     * "level1" (nested) &rarr; "level1.level2" (nested, one level deeper) — mirrors the real
     * {@code products.variants} shape. 2 roots across 2 generations, asymmetric level1/level2 counts,
     * interleaved by the row-id mapping exactly as the depth-1 test does. Verifies: (a) root-only
     * row-id stamping still holds with 2 levels of nesting present, (b) each level's own
     * {@code _nested_path} postings correctly separate level1 docs from level1.level2 docs, and (c) the
     * ASCENDING-docId order within {@code level1.level2}'s postings, grouped per root, reproduces parse
     * order — this is the exact invariant {@code NestedChildOrdinalMap}'s chained-offset design (see
     * {@code MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md} &sect;1.3 finding 2) depends on surviving a REAL
     * background merge, not just a single-generation flush (already covered by the companion
     * {@code LuceneNestedBlockOrderPhase0Tests.testDepth2NestedOrderSurvivesPerGenerationForceMerge}).
     */
    public void testMergeInterleavesDepth2NestedBlocksAcrossGenerations() throws IOException {
        // gen=1: root A (rowId=0) has level1 elements each with some level2 leaves:
        //   level1[0] -> level2 leaves [a-1-0-0, a-1-0-1], level1[1] -> level2 leaves [a-1-1-0]
        writeDepth2NestedSegment(writer, 1L, List.of(List.of(List.of("a-1-0-0", "a-1-0-1"), List.of("a-1-1-0"))));
        // gen=2: root B (rowId=0) has level1 elements:
        //   level1[0] -> level2 leaves [] (empty), level1[1] -> level2 leaves [b-1-1-0, b-1-1-1]
        writeDepth2NestedSegment(writer, 2L, List.of(List.of(List.of(), List.of("b-1-1-0", "b-1-1-1"))));
        writer.commit();

        // Swap: gen=1 root A -> new row 1, gen=2 root B -> new row 0.
        long[] mappingArray = new long[] { 1, 0 };
        Map<Long, Integer> genOffsets = Map.of(1L, 0, 2L, 1);
        Map<Long, Integer> genSizes = Map.of(1L, 1, 2L, 1);
        RowIdMapping rowIdMapping = new PackedRowIdMapping(mappingArray, genOffsets, genSizes);

        LuceneMerger merger = new LuceneMerger(writer, new LuceneDataFormat(), dataPath, new LuceneShardStatsTracker());
        SegmentInfos infos = getSegmentInfos(writer);
        List<Segment> segments = buildSegments(infos);
        MergeInput input = MergeInput.builder().segments(segments).rowIdMapping(rowIdMapping).newWriterGeneration(11L).build();

        MergeResult result;
        boolean threw = false;
        String thrownMessage = null;
        try {
            result = merger.merge(input);
        } catch (RuntimeException | AssertionError | IOException e) {
            threw = true;
            thrownMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
            result = null;
        }

        if (threw) {
            fail(
                "Cross-generation merge with DEPTH-2 nested document blocks present threw ("
                    + thrownMessage
                    + "). Case (c) per MULTI_LEVEL_LUCENE_DELEGATION_PLAN.md's Phase 0 classification — "
                    + "STOP and re-open root-cause analysis before proceeding with that plan."
            );
            return;
        }

        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            LeafReaderContext mergedLeaf = null;
            for (LeafReaderContext ctx : reader.leaves()) {
                if (mergedLeaf == null || ctx.reader().maxDoc() > mergedLeaf.reader().maxDoc()) {
                    mergedLeaf = ctx;
                }
            }
            assertNotNull("Should have at least one leaf", mergedLeaf);
            var leaf = mergedLeaf.reader();
            dumpDocLayout(leaf, leaf.maxDoc());

            // Root row IDs: exactly 2 roots, sequential 0,1 in ascending docId order (root B first
            // since it was mapped to new row 0).
            SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(ROW_ID_FIELD);
            assertNotNull("__row_id__ doc values should exist", rowIdDV);
            List<Long> rootRowIdsInDocOrder = new ArrayList<>();
            for (int d = rowIdDV.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = rowIdDV.nextDoc()) {
                rootRowIdsInDocOrder.add(rowIdDV.nextValue());
            }
            assertEquals(
                "exactly 2 root docs should carry __row_id__ (level1 AND level1.level2 docs must both be excluded)",
                List.of(0L, 1L),
                rootRowIdsInDocOrder
            );

            // level1 docs: root A had 2, root B had 2 -> 4 total.
            List<Integer> level1DocIds = collectDocIdsForPath(leaf, "level1");
            assertEquals("level1 doc count", 4, level1DocIds.size());

            // level1.level2 docs, in ascending docId order, must reproduce parse order WITHIN each
            // root's post-reorder position: root B (new row 0, first in doc order) contributes
            // [] then [b-1-1-0,b-1-1-1] = [b-1-1-0,b-1-1-1]; root A (new row 1, second) contributes
            // [a-1-0-0,a-1-0-1] then [a-1-1-0] = [a-1-0-0,a-1-0-1,a-1-1-0].
            List<Integer> level2DocIds = collectDocIdsForPath(leaf, "level1.level2");
            List<String> level2ValuesInDocIdOrder = new ArrayList<>();
            for (int docId : level2DocIds) {
                level2ValuesInDocIdOrder.add(readLeafValue(leaf, docId));
            }
            List<String> expected = List.of("b-1-1-0", "b-1-1-1", "a-1-0-0", "a-1-0-1", "a-1-1-0");
            assertEquals(
                "level1.level2 leaf values in ascending docId order must reproduce per-root parse order, "
                    + "grouped by post-reorder root position — this is the EXACT invariant the multi-level "
                    + "Lucene delegation design's chained-offset scheme depends on",
                expected,
                level2ValuesInDocIdOrder
            );
        }
    }

    // --- helpers (adapted from LuceneMergerTests' reflection-based segment plumbing) ---

    /**
     * Writes {@code roots.size()} root docs to {@code w}, each with a nested "children" block whose
     * size is {@code roots.get(i).size()} and whose child docs carry a "leaf" keyword field with the
     * given values, in parse order. Root {@code i} gets {@code __row_id__ = i} (segment-local, matching
     * how the real system assigns row IDs before any cross-generation remap).
     */
    private void writeNestedSegment(MergeIndexWriter w, long generation, List<List<String>> roots) throws IOException {
        for (int r = 0; r < roots.size(); r++) {
            List<Document> block = new ArrayList<>();
            for (String leafValue : roots.get(r)) {
                Document child = new Document();
                child.add(new org.apache.lucene.document.StringField(NestedPathFieldMapper.NAME, "children", org.apache.lucene.document.Field.Store.NO));
                child.add(new StringField("leaf", leafValue, org.apache.lucene.document.Field.Store.NO));
                block.add(child);
            }
            Document root = new Document();
            root.add(new SortedNumericDocValuesField(ROW_ID_FIELD, r));
            block.add(root); // root last, matching LuceneDocumentInput.getDocumentBlock's contract
            w.addDocuments(block);
        }
        w.flush();
        setWriterGenerationOnLatestSegment(w, generation);
    }

    /**
     * Depth-2 variant of {@link #writeNestedSegment}: {@code roots} is a list of root specs, each a
     * list of level1-element specs, each itself a list of level2 leaf values. Block layout follows the
     * same post-order convention as {@code LuceneDocumentInput} (deepest children first, each level's
     * own doc immediately after its children, root last): for one root with level1 elements
     * [[x,y],[z]], the block is [level2:x, level2:y, level1(empty doc), level2:z, level1(empty doc),
     * root] — matching how {@code startNestedChild}/{@code endNestedChild} would emit it in real parse
     * order (level1 itself carries no fields here since this test only cares about ordering, not field
     * content at that level).
     */
    private void writeDepth2NestedSegment(MergeIndexWriter w, long generation, List<List<List<String>>> roots) throws IOException {
        for (int r = 0; r < roots.size(); r++) {
            List<Document> block = new ArrayList<>();
            for (List<String> level1Elem : roots.get(r)) {
                for (String leafValue : level1Elem) {
                    Document level2Doc = new Document();
                    level2Doc.add(
                        new org.apache.lucene.document.StringField(
                            NestedPathFieldMapper.NAME,
                            "level1.level2",
                            org.apache.lucene.document.Field.Store.NO
                        )
                    );
                    level2Doc.add(new StringField("leaf", leafValue, org.apache.lucene.document.Field.Store.NO));
                    block.add(level2Doc);
                }
                Document level1Doc = new Document();
                level1Doc.add(
                    new org.apache.lucene.document.StringField(NestedPathFieldMapper.NAME, "level1", org.apache.lucene.document.Field.Store.NO)
                );
                block.add(level1Doc);
            }
            Document root = new Document();
            root.add(new SortedNumericDocValuesField(ROW_ID_FIELD, r));
            block.add(root);
            w.addDocuments(block);
        }
        w.flush();
        setWriterGenerationOnLatestSegment(w, generation);
    }

    private static String readLeafValue(org.apache.lucene.index.LeafReader leaf, int docId) throws IOException {
        Terms terms = leaf.terms("leaf");
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

    @SuppressForbidden(reason = "Need reflection to stamp writer_generation on segments for testing")
    private void setWriterGenerationOnLatestSegment(MergeIndexWriter w, long generation) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = org.apache.lucene.index.IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segInfos = (SegmentInfos) segInfosField.get(w);
            if (segInfos.size() > 0) {
                SegmentCommitInfo lastSegment = segInfos.asList().get(segInfos.size() - 1);
                if (lastSegment.info.getAttribute(WRITER_GENERATION_ATTRIBUTE) == null) {
                    lastSegment.info.putAttribute(WRITER_GENERATION_ATTRIBUTE, String.valueOf(generation));
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to set writer_generation attribute via reflection", e);
        }
    }

    @SuppressForbidden(reason = "Need reflection to access live SegmentInfos for test assertions")
    private SegmentInfos getSegmentInfos(MergeIndexWriter w) throws IOException {
        try {
            java.lang.reflect.Field segInfosField = org.apache.lucene.index.IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            return (SegmentInfos) segInfosField.get(w);
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to access segmentInfos via reflection", e);
        }
    }

    private List<Segment> buildSegments(SegmentInfos infos) {
        List<Segment> segments = new ArrayList<>();
        for (SegmentCommitInfo sci : infos.asList()) {
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
            if (genAttr != null) {
                long generation = Long.parseLong(genAttr);
                segments.add(Segment.builder(generation).build());
            }
        }
        return segments;
    }

    private static List<Integer> collectDocIdsForPath(org.apache.lucene.index.LeafReader leaf, String nestedPath) throws IOException {
        List<Integer> docIds = new ArrayList<>();
        Terms terms = leaf.terms(NestedPathFieldMapper.NAME);
        if (terms == null) {
            return docIds;
        }
        TermsEnum te = terms.iterator();
        if (te.seekExact(new BytesRef(nestedPath)) == false) {
            return docIds;
        }
        PostingsEnum pe = te.postings(null, PostingsEnum.NONE);
        for (int d = pe.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = pe.nextDoc()) {
            docIds.add(d);
        }
        return docIds;
    }

    private void dumpDocLayout(org.apache.lucene.index.LeafReader leaf, int maxDoc) throws IOException {
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
        SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(ROW_ID_FIELD);
        if (rowIdDV != null) {
            for (int d = rowIdDV.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = rowIdDV.nextDoc()) {
                rowIdPerDoc[d] = rowIdDV.nextValue();
            }
        }
        StringBuilder sb = new StringBuilder("PHASE0 MERGER DIAGNOSTIC DOC LAYOUT (docId -> role):\n");
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
}
