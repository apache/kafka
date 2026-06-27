/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.openlineage.visitor;

import org.apache.kafka.connect.openlineage.ConnectorLineage;
import org.apache.kafka.connect.openlineage.VisitorRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the {@link VisitorRegistry} and all built-in visitors.
 */
public class VisitorRegistryTest {

    private VisitorRegistry registry;

    @BeforeEach
    public void setUp() {
        registry = new VisitorRegistry();
    }

    // ---------------------------------------------------------------
    // JDBC Source
    // ---------------------------------------------------------------

    @Test
    public void testJdbcSourceVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        config.put("connection.url", "jdbc:postgresql://dbhost:5432/mydb");
        config.put("table.include.list", "users,orders");
        config.put("topic.prefix", "jdbc_");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("JDBC_SOURCE", lineage.jobType());
        assertEquals(2, lineage.inputs().size());
        assertEquals("postgres://dbhost:5432", lineage.inputs().get(0).namespace());
        assertEquals("mydb.public.users", lineage.inputs().get(0).name());
        assertEquals("mydb.public.orders", lineage.inputs().get(1).name());
        assertFalse(lineage.outputs().isEmpty());
    }

    @Test
    public void testJdbcSourceQueryMode() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        config.put("connection.url", "jdbc:mysql://host:3306/analytics");
        config.put("query", "SELECT * FROM events WHERE ts > ?");
        config.put("topics", "events_topic");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("JDBC_SOURCE", lineage.jobType());
        assertEquals(1, lineage.inputs().size());
        assertEquals("analytics.query", lineage.inputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // JDBC Sink
    // ---------------------------------------------------------------

    @Test
    public void testJdbcSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
        config.put("connection.url", "jdbc:postgresql://dbhost:5432/warehouse");
        config.put("topics", "orders,payments");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("JDBC_SINK", lineage.jobType());
        assertEquals(2, lineage.inputs().size());
        assertEquals(2, lineage.outputs().size());
        assertEquals("postgres://dbhost:5432", lineage.outputs().get(0).namespace());
        assertEquals("warehouse.public.orders", lineage.outputs().get(0).name());
    }

    @Test
    public void testJdbcSinkWithTableFormat() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
        config.put("connection.url", "jdbc:postgresql://dbhost:5432/warehouse");
        config.put("topics", "orders");
        config.put("table.name.format", "stg_${topic}");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("warehouse.public.stg_orders", lineage.outputs().get(0).name());
    }

    @Test
    public void testJdbcSinkMySqlIsTwoPart() {
        // MySQL OpenLineage naming is database.table (no schema level).
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
        config.put("connection.url", "jdbc:mysql://dbhost:3306/warehouse");
        config.put("topics", "orders");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("mysql://dbhost:3306", lineage.outputs().get(0).namespace());
        assertEquals("warehouse.orders", lineage.outputs().get(0).name());
    }

    @Test
    public void testJdbcSinkSchemaQualifiedTableNotDoubled() {
        // A table.name.format that is already schema-qualified must not get a
        // second (default) schema injected.
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
        config.put("connection.url", "jdbc:postgresql://dbhost:5432/warehouse");
        config.put("topics", "orders");
        config.put("table.name.format", "sales.${topic}");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("warehouse.sales.orders", lineage.outputs().get(0).name());
    }

    @Test
    public void testKafkaNamespaceUsesBootstrapServers() {
        // When bootstrap.servers is present (LifecycleMonitor injects the worker
        // value), the Kafka topic namespace must use the real broker rather than
        // the kafka://localhost:9092 fallback.
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
        config.put("connection.url", "jdbc:postgresql://dbhost:5432/warehouse");
        config.put("topics", "orders");
        config.put("bootstrap.servers", "broker-1:9092,broker-2:9092");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("kafka://broker-1:9092", lineage.inputs().get(0).namespace());
        assertEquals("orders", lineage.inputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // S3 Sink
    // ---------------------------------------------------------------

    @Test
    public void testS3SinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.s3.S3SinkConnector");
        config.put("topics", "events,logs");
        config.put("s3.bucket.name", "my-data-lake");
        config.put("topics.dir", "raw");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("S3_SINK", lineage.jobType());
        assertEquals(2, lineage.inputs().size());
        assertEquals(2, lineage.outputs().size());
        assertEquals("s3://my-data-lake", lineage.outputs().get(0).namespace());
        assertEquals("raw/events", lineage.outputs().get(0).name());
        assertEquals("raw/logs", lineage.outputs().get(1).name());
    }

    // ---------------------------------------------------------------
    // GCS Sink
    // ---------------------------------------------------------------

    @Test
    public void testGcsSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.gcs.GcsSinkConnector");
        config.put("topics", "events");
        config.put("gcs.bucket.name", "my-gcs-bucket");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("GCS_SINK", lineage.jobType());
        assertEquals("gs://my-gcs-bucket", lineage.outputs().get(0).namespace());
        assertEquals("topics/events", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // Azure Blob Sink
    // ---------------------------------------------------------------

    @Test
    public void testAzureBlobSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "io.confluent.connect.azblob.AzureBlobStorageSinkConnector");
        config.put("topics", "events");
        config.put("azblob.account.name", "myaccount");
        config.put("azblob.container.name", "mycontainer");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("AZURE_BLOB_SINK", lineage.jobType());
        assertEquals("abfss://mycontainer@myaccount.dfs.core.windows.net",
            lineage.outputs().get(0).namespace());
    }

    // ---------------------------------------------------------------
    // HDFS Sink
    // ---------------------------------------------------------------

    @Test
    public void testHdfsSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.hdfs.HdfsSinkConnector");
        config.put("topics", "events");
        config.put("hdfs.url", "hdfs://namenode:8020");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("HDFS_SINK", lineage.jobType());
        assertEquals("hdfs://namenode:8020", lineage.outputs().get(0).namespace());
    }

    @Test
    public void testHdfs3SinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.hdfs3.Hdfs3SinkConnector");
        config.put("topics", "events");
        config.put("hdfs.url", "hdfs://namenode:8020/");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("HDFS_SINK", lineage.jobType());
        // Trailing slash should be stripped
        assertEquals("hdfs://namenode:8020", lineage.outputs().get(0).namespace());
    }

    // ---------------------------------------------------------------
    // Debezium
    // ---------------------------------------------------------------

    @Test
    public void testDebeziumMySqlVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        config.put("database.hostname", "mysql-host");
        config.put("database.port", "3306");
        config.put("database.dbname", "inventory");
        config.put("table.include.list", "inventory.products,inventory.orders");
        config.put("topic.prefix", "dbserver1");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("DEBEZIUM_MYSQL", lineage.jobType());
        assertEquals(2, lineage.inputs().size());
        assertEquals("mysql://mysql-host:3306", lineage.inputs().get(0).namespace());
        assertEquals("inventory.products", lineage.inputs().get(0).name());
    }

    @Test
    public void testDebeziumPostgresVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "io.debezium.connector.postgresql.PostgresConnector");
        config.put("database.hostname", "pg-host");
        config.put("database.port", "5432");
        config.put("table.include.list", "public.users");
        config.put("topic.prefix", "pgserver");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("DEBEZIUM_POSTGRESQL", lineage.jobType());
        assertEquals("postgres://pg-host:5432", lineage.inputs().get(0).namespace());
    }

    @Test
    public void testDebeziumWithDefaultPort() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "io.debezium.connector.sqlserver.SqlServerConnector");
        config.put("database.hostname", "sql-host");
        config.put("database.dbname", "mydb");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("DEBEZIUM_SQLSERVER", lineage.jobType());
        // Should use default port 1433
        assertEquals("sqlserver://sql-host:1433", lineage.inputs().get(0).namespace());
    }

    // ---------------------------------------------------------------
    // MongoDB Source
    // ---------------------------------------------------------------

    @Test
    public void testMongoDbSourceVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "com.mongodb.kafka.connect.MongoSourceConnector");
        config.put("connection.uri", "mongodb://mongo-host:27017");
        config.put("database", "mydb");
        config.put("collection", "events");
        config.put("topic.prefix", "mongo");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("MONGODB_SOURCE", lineage.jobType());
        assertEquals(1, lineage.inputs().size());
        assertEquals("mongodb://mongo-host:27017", lineage.inputs().get(0).namespace());
        assertEquals("mydb.events", lineage.inputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // MongoDB Sink
    // ---------------------------------------------------------------

    @Test
    public void testMongoDbSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "com.mongodb.kafka.connect.MongoSinkConnector");
        config.put("connection.uri", "mongodb://mongo-host:27017");
        config.put("database", "analytics");
        config.put("collection", "events");
        config.put("topics", "events_topic");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("MONGODB_SINK", lineage.jobType());
        assertEquals(1, lineage.inputs().size());
        assertEquals(1, lineage.outputs().size());
        assertEquals("analytics.events", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // Elasticsearch Sink
    // ---------------------------------------------------------------

    @Test
    public void testElasticsearchSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector");
        config.put("topics", "logs,metrics");
        config.put("connection.url", "http://es-host:9200");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("ELASTICSEARCH_SINK", lineage.jobType());
        assertEquals(2, lineage.inputs().size());
        assertEquals(2, lineage.outputs().size());
        assertEquals("elasticsearch://es-host:9200", lineage.outputs().get(0).namespace());
        assertEquals("logs", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // BigQuery Sink
    // ---------------------------------------------------------------

    @Test
    public void testBigQuerySinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector");
        config.put("topics", "events");
        config.put("project", "my-gcp-project");
        config.put("defaultDataset", "raw_data");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("BIGQUERY_SINK", lineage.jobType());
        assertEquals(1, lineage.outputs().size());
        assertEquals("bigquery", lineage.outputs().get(0).namespace());
        assertEquals("my-gcp-project.raw_data.events", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // Snowflake Sink
    // ---------------------------------------------------------------

    @Test
    public void testSnowflakeSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "com.snowflake.kafka.connector.SnowflakeSinkConnector");
        config.put("topics", "events");
        config.put("snowflake.url.name", "myaccount.snowflakecomputing.com");
        config.put("snowflake.database.name", "ANALYTICS");
        config.put("snowflake.schema.name", "RAW");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("SNOWFLAKE_SINK", lineage.jobType());
        assertEquals(1, lineage.outputs().size());
        assertEquals("snowflake://myaccount.snowflakecomputing.com",
            lineage.outputs().get(0).namespace());
        assertEquals("ANALYTICS.RAW.events", lineage.outputs().get(0).name());
    }

    @Test
    public void testSnowflakeSinkWithTableMap() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "com.snowflake.kafka.connector.SnowflakeSinkConnector");
        config.put("topics", "events,orders");
        config.put("snowflake.url.name", "myaccount.snowflakecomputing.com");
        config.put("snowflake.database.name", "DB");
        config.put("snowflake.schema.name", "PUBLIC");
        config.put("snowflake.topic2table.map", "events:EVENT_TABLE,orders:ORDER_TABLE");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals(2, lineage.outputs().size());
        assertEquals("DB.PUBLIC.EVENT_TABLE", lineage.outputs().get(0).name());
        assertEquals("DB.PUBLIC.ORDER_TABLE", lineage.outputs().get(1).name());
    }

    // ---------------------------------------------------------------
    // Cassandra Sink
    // ---------------------------------------------------------------

    @Test
    public void testCassandraSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "com.datastax.oss.kafka.sink.CassandraSinkConnector");
        config.put("topics", "events");
        config.put("contactPoints", "cass-host:9042");
        config.put("topic.events.mykeyspace.mytable.mapping", "col1=value.col1");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("CASSANDRA_SINK", lineage.jobType());
        assertEquals(1, lineage.outputs().size());
        assertEquals("cassandra://cass-host:9042", lineage.outputs().get(0).namespace());
        assertEquals("mykeyspace.mytable", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // Redshift Sink
    // ---------------------------------------------------------------

    @Test
    public void testRedshiftSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "io.confluent.connect.redshift.RedshiftSinkConnector");
        config.put("topics", "events");
        config.put("connection.url",
            "jdbc:redshift://my-cluster.abc.us-east-1.redshift.amazonaws.com:5439/mydb");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("REDSHIFT_SINK", lineage.jobType());
        assertEquals(1, lineage.outputs().size());
        assertEquals(
            "redshift://my-cluster.abc.us-east-1.redshift.amazonaws.com:5439",
            lineage.outputs().get(0).namespace());
        assertEquals("mydb.public.events", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // MirrorMaker
    // ---------------------------------------------------------------

    @Test
    public void testMirrorMakerVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class",
            "org.apache.kafka.connect.mirror.MirrorSourceConnector");
        config.put("source.cluster.alias", "us-east");
        config.put("target.cluster.alias", "us-west");
        config.put("source.cluster.bootstrap.servers", "east-broker:9092");
        config.put("bootstrap.servers", "west-broker:9092");
        config.put("topics", "orders,payments");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("MIRROR_SOURCE", lineage.jobType());
        assertEquals(2, lineage.inputs().size());
        assertEquals(2, lineage.outputs().size());
        assertEquals("kafka://east-broker:9092", lineage.inputs().get(0).namespace());
        assertEquals("orders", lineage.inputs().get(0).name());
        assertEquals("kafka://west-broker:9092", lineage.outputs().get(0).namespace());
        assertEquals("us-east.orders", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // HTTP Sink
    // ---------------------------------------------------------------

    @Test
    public void testHttpSinkVisitor() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.confluent.connect.http.HttpSinkConnector");
        config.put("topics", "events");
        config.put("http.api.url", "https://api.example.com:8443/v1/ingest");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("HTTP_SINK", lineage.jobType());
        assertEquals(1, lineage.inputs().size());
        assertEquals(1, lineage.outputs().size());
        assertEquals("https://api.example.com:8443", lineage.outputs().get(0).namespace());
        assertEquals("/v1/ingest", lineage.outputs().get(0).name());
    }

    // ---------------------------------------------------------------
    // Generic fallback
    // ---------------------------------------------------------------

    @Test
    public void testGenericVisitorForUnknownSink() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "com.example.CustomSinkConnector");
        config.put("topics", "my-topic");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("CustomSinkConnector", lineage.jobType());
        // Generic visitor should identify this as a sink from the class name
        assertEquals(1, lineage.inputs().size());
        assertEquals("my-topic", lineage.inputs().get(0).name());
    }

    @Test
    public void testGenericVisitorForUnknownSource() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "com.example.CustomSourceConnector");
        config.put("topics", "my-topic");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertEquals("CustomSourceConnector", lineage.jobType());
        // Generic visitor should identify this as a source from the class name
        assertEquals(1, lineage.outputs().size());
        assertEquals("my-topic", lineage.outputs().get(0).name());
    }

    @Test
    public void testGenericVisitorWithEmptyConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "com.example.Something");

        ConnectorLineage lineage = registry.extractLineage(config);
        assertNotNull(lineage);
        assertEquals("Something", lineage.jobType());
    }

    // ---------------------------------------------------------------
    // Registry structure
    // ---------------------------------------------------------------

    @Test
    public void testGenericVisitorIsLastFallback() {
        // Verify GenericVisitor catches unknown connectors as a fallback
        Map<String, String> unknownConfig = new HashMap<>();
        unknownConfig.put("connector.class", "com.example.CompletelyUnknownConnector");
        unknownConfig.put("topics", "test-topic");
        unknownConfig.put("bootstrap.servers", "kafka:9092");

        ConnectorLineage lineage = registry.extractLineage(unknownConfig);
        // GenericVisitor should handle it — outputs contain the topic for source-like names
        assertFalse(lineage.inputs().isEmpty() && lineage.outputs().isEmpty());
    }
}
