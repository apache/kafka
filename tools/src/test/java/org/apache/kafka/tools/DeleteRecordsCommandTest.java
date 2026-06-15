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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteRecordsCommandTest {

    @ClusterTest
    public void testCommand(ClusterInstance cluster) throws Exception {
        Map<String, Object> adminProps = new HashMap<>();

        adminProps.put(AdminClientConfig.RETRIES_CONFIG, 1);

        try (Admin admin = cluster.admin(adminProps)) {
            assertThrows(
                AdminCommandFailedException.class,
                () -> DeleteRecordsCommand.execute(admin, "{\"partitions\":[" +
                    "{\"topic\":\"t\", \"partition\":0, \"offset\":1}," +
                    "{\"topic\":\"t\", \"partition\":0, \"offset\":1}]" +
                    "}", System.out),
                "Offset json file contains duplicate topic partitions: t-0"
            );

            admin.createTopics(Set.of(new NewTopic("t", 1, (short) 1))).all().get();

            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<?, String> producer = new KafkaProducer<>(props)) {
                producer.send(new ProducerRecord<>("t", "1")).get();
                producer.send(new ProducerRecord<>("t", "2")).get();
                producer.send(new ProducerRecord<>("t", "3")).get();
            }

            executeAndAssertOutput(
                "{\"partitions\":[{\"topic\":\"t\", \"partition\":0, \"offset\":1}]}",
                "partition: t-0\tlow_watermark: 1",
                admin
            );

            executeAndAssertOutput(
                "{\"partitions\":[{\"topic\":\"t\", \"partition\":42, \"offset\":42}]}",
                "partition: t-42\terror",
                admin
            );
        }
    }

    private static void executeAndAssertOutput(String json, String expOut, Admin admin) {
        String output = ToolsTestUtils.captureStandardOut(() -> {
            try {
                DeleteRecordsCommand.execute(admin, json, System.out);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        assertTrue(output.contains(expOut));
    }

    /**
     * Unit test of {@link DeleteRecordsCommand} tool.
     */

    @Test
    public void testOffsetFileNotExists() {
        assertThrows(IOException.class, () -> DeleteRecordsCommand.execute(new String[]{
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file"
        }, System.out));
        assertEquals(1, DeleteRecordsCommand.mainNoExit(
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file"));
    }

    @Test
    public void testCommandConfigNotExists() {
        assertThrows(NoSuchFileException.class, () -> DeleteRecordsCommand.execute(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file",
            "--command-config", "/another/not/existing/file"
        }, System.out));
        assertEquals(1, DeleteRecordsCommand.mainNoExit(
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file",
            "--command-config", "/another/not/existing/file"));
    }

    @Test
    public void testWrongVersion() {
        assertCommandThrows(JsonProcessingException.class, "{\"version\":\"string\"}");
        assertCommandThrows(AdminOperationException.class, "{\"version\":2}");
    }

    @Test
    public void testWrongPartitions() {
        assertCommandThrows(AdminOperationException.class, "{\"version\":1}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":2}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":{}}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":[{}]}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":[{\"topic\":\"t\"}]}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":[{\"topic\":\"t\", \"partition\": \"\"}]}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":[{\"topic\":\"t\", \"partition\": 0}]}");
        assertCommandThrows(JsonProcessingException.class, "{\"partitions\":[{\"topic\":\"t\", \"offset\":0}]}");
    }

    @Test
    public void testParse() throws Exception {
        Map<TopicPartition, List<Long>> res = DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(
            "{\"partitions\":[" +
                "{\"topic\":\"t\", \"partition\":0, \"offset\":0}," +
                "{\"topic\":\"t\", \"partition\":1, \"offset\":1, \"ignored\":\"field\"}," +
                "{\"topic\":\"t\", \"partition\":0, \"offset\":2}," +
                "{\"topic\":\"t\", \"partition\":0, \"offset\":0}" +
                "]}"
        );

        assertEquals(2, res.size());
        assertEquals(List.of(0L, 2L, 0L), res.get(new TopicPartition("t", 0)));
        assertEquals(List.of(1L), res.get(new TopicPartition("t", 1)));
    }

    @Test
    public void testExecuteSuccessOutputWithMockAdmin() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);

        KafkaFuture<DeletedRecords> future = KafkaFuture.completedFuture(new DeletedRecords(5L));

        DeleteRecordsResult deleteResult = mock(DeleteRecordsResult.class);
        when(deleteResult.lowWatermarks()).thenReturn(Map.of(tp, future));

        Admin mockAdmin = mock(Admin.class);
        when(mockAdmin.deleteRecords(any())).thenReturn(deleteResult);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DeleteRecordsCommand.execute(
            mockAdmin,
            "{\"partitions\":[{\"topic\":\"t\",\"partition\":0,\"offset\":5}]}",
            new PrintStream(bout)
        );

        String output = bout.toString();
        assertTrue(output.contains("Executing records delete operation"));
        assertTrue(output.contains("Records delete operation completed:"));
        assertTrue(output.contains("partition: t-0\tlow_watermark: 5"));
    }

    @Test
    public void testExecutePartitionErrorWithMockAdmin() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);

        KafkaFutureImpl<DeletedRecords> future = new KafkaFutureImpl<>();
        future.completeExceptionally(new RuntimeException("Partition not found"));

        DeleteRecordsResult deleteResult = mock(DeleteRecordsResult.class);
        when(deleteResult.lowWatermarks()).thenReturn(Map.of(tp, future));

        Admin mockAdmin = mock(Admin.class);
        when(mockAdmin.deleteRecords(any())).thenReturn(deleteResult);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DeleteRecordsCommand.execute(
            mockAdmin,
            "{\"partitions\":[{\"topic\":\"t\",\"partition\":0,\"offset\":1}]}",
            new PrintStream(bout)
        );

        String output = bout.toString();
        assertTrue(output.contains("partition: t-0\terror:"));
    }

    @Test
    public void testExecuteDuplicatePartitionsWithoutCluster() {
        Admin mockAdmin = mock(Admin.class);

        assertThrows(
            AdminCommandFailedException.class,
            () -> DeleteRecordsCommand.execute(
                mockAdmin,
                "{\"partitions\":[" +
                    "{\"topic\":\"t\",\"partition\":0,\"offset\":1}," +
                    "{\"topic\":\"t\",\"partition\":0,\"offset\":2}]}",
                System.out
            )
        );
    }

    @Test
    public void testParseInvalidJsonThrows() {
        assertCommandThrows(AdminOperationException.class, "not-valid-json");
        assertCommandThrows(AdminOperationException.class, "");
    }

    /**
     * Asserts that {@link DeleteRecordsCommand#parseOffsetJsonStringWithoutDedup(String)} throws {@link AdminOperationException}.
     * @param jsonData Data to check.
     */
    private static void assertCommandThrows(Class<? extends Exception> expectedException, String jsonData) {
        assertThrows(
            expectedException,
            () -> DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(jsonData)
        );
    }
}