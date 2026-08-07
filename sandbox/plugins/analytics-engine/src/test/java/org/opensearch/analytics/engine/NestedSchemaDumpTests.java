/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.opensearch.Version;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Dumps the exact Calcite row type OpenSearchSchemaBuilder produces for the 4 nested-POC
 * test indices (blogs / ecommerce / logs / employees) — the "Mustang schema" side of the
 * Mustang-vs-vanilla schema comparison. Not an assertion suite; the output is the artifact.
 */
public class NestedSchemaDumpTests extends OpenSearchTestCase {

    public void testDumpNestedPocSchemas() throws Exception {
        String blogs = """
            {"properties": {
              "title": {"type": "keyword"}, "views": {"type": "integer"},
              "comments": {"type": "nested", "properties": {
                "author": {"type": "keyword"}, "score": {"type": "integer"}}}}}
            """;
        String ecommerce = """
            {"properties": {
              "product_name": {"type": "keyword"}, "price": {"type": "double"},
              "category": {"type": "keyword"}, "in_stock": {"type": "boolean"},
              "reviews": {"type": "nested", "properties": {
                "reviewer": {"type": "keyword"}, "rating": {"type": "integer"},
                "comment": {"type": "text"}, "helpful_votes": {"type": "integer"}}}}}
            """;
        String logs = """
            {"properties": {
              "service": {"type": "keyword"}, "level": {"type": "keyword"},
              "http_status": {"type": "integer"}, "response_time": {"type": "double"},
              "spans": {"type": "nested", "properties": {
                "operation": {"type": "keyword"}, "duration_ms": {"type": "integer"},
                "status": {"type": "keyword"}, "bytes": {"type": "integer"}}}}}
            """;
        String employees = """
            {"properties": {
              "name": {"type": "keyword"}, "department": {"type": "keyword"},
              "salary": {"type": "integer"}, "years_exp": {"type": "integer"},
              "skills": {"type": "nested", "properties": {
                "name": {"type": "keyword"}, "level": {"type": "integer"},
                "years_used": {"type": "integer"}, "certified": {"type": "keyword"}}}}}
            """;

        Metadata.Builder md = Metadata.builder();
        for (Map.Entry<String, String> e : Map.of("blogs", blogs, "ecommerce", ecommerce, "logs", logs, "employees", employees)
            .entrySet()) {
            md.put(
                IndexMetadata.builder(e.getKey())
                    .settings(settings(Version.CURRENT).put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                    .putMapping(e.getValue())
            );
        }
        ClusterState state = ClusterState.builder(new ClusterName("dump")).metadata(md).build();
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(state);

        StringBuilder out = new StringBuilder("\n===== MUSTANG CALCITE SCHEMA DUMP =====\n");
        for (String index : new String[] { "blogs", "ecommerce", "logs", "employees" }) {
            Table table = schema.getTable(index);
            assertNotNull(table);
            RelDataType rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
            out.append("\n--- ").append(index).append(" ---\n");
            for (RelDataTypeField f : rowType.getFieldList()) {
                out.append(String.format("  $%d  %-14s %s%n", f.getIndex(), f.getName(), f.getType().getFullTypeString()));
            }
        }
        logger.info(out.toString());
        // Print to stdout too so the gradle test-output XML captures it plainly.
        System.out.println(out);
    }
}
