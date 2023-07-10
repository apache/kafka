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

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.KRAFT)
@Tag("integration")
public class DeleteRecordsCommandTest {

    private final ClusterInstance cluster;
    public DeleteRecordsCommandTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(clusterType = Type.ZK)
    public void testCommand() throws Exception {
        Properties adminProps = new Properties();

        adminProps.put(AdminClientConfig.RETRIES_CONFIG, 1);

        try (Admin admin = cluster.createAdminClient(adminProps)) {
            assertThrows(
                AdminCommandFailedException.class,
                () -> DeleteRecordsCommand.execute0(admin, "{\"partitions\":[" +
                    "{\"topic\":\"t\", \"partition\":0, \"offset\":1}," +
                    "{\"topic\":\"t\", \"partition\":0, \"offset\":1}]" +
                    "}", System.out),
                "Offset json file contains duplicate topic partitions: t-0"
            );

            admin.createTopics(Collections.singleton(new NewTopic("t", 1, (short) 1))).all().get();

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
        String output =
            ToolsTestUtils.captureStandardOut(() -> DeleteRecordsCommand.execute0(admin, json, System.out));
        assertTrue(output.contains(expOut));
    }
}

/**
 * Unit test of {@link DeleteRecordsCommand} tool.
 */
class DeleteRecordsCommandUnitTest {
    @Test
    public void testOffsetFileNotExists() {
        assertThrows(IOException.class, () -> DeleteRecordsCommand.main(new String[]{
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file"
        }));
    }

    @Test
    public void testCommandConfigNotExists() {
        assertThrows(NoSuchFileException.class, () -> DeleteRecordsCommand.main(new String[] {
            "--bootstrap-server", "localhost:9092",
            "--offset-json-file", "/not/existing/file",
            "--command-config", "/another/not/existing/file"
        }));
    }

    @Test
    public void testWrongVersion() {
        assertThrowsAdminOperationException("{\"version\":\"string\"}");
        assertThrowsAdminOperationException("{\"version\":2}");
    }

    @Test
    public void testWrongPartitions() {
        assertThrowsAdminOperationException("{\"version\":1}");
        assertThrowsAdminOperationException("{\"partitions\":2}");
        assertThrowsAdminOperationException("{\"partitions\":{}}");
        assertThrowsAdminOperationException("{\"partitions\":[{}]}");
        assertThrowsAdminOperationException("{\"partitions\":[{\"topic\":\"t\"}]}");
        assertThrowsAdminOperationException("{\"partitions\":[{\"topic\":\"t\", \"partition\": \"\"}]}");
        assertThrowsAdminOperationException("{\"partitions\":[{\"topic\":\"t\", \"partition\": 0}]}");
        assertThrowsAdminOperationException("{\"partitions\":[{\"topic\":\"t\", \"offset\":0}]}");
    }

    @Test
    public void testParse() {
        Collection<DeleteRecordsCommand.Tuple<TopicPartition, Long>> res = DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(
            "{\"partitions\":[" +
                "{\"topic\":\"t\", \"partition\":0, \"offset\":0}," +
                "{\"topic\":\"t\", \"partition\":1, \"offset\":1}," +
                "{\"topic\":\"t\", \"partition\":0, \"offset\":2}," +
                "{\"topic\":\"t\", \"partition\":0, \"offset\":0}" +
            "]}"
        );

        assertEquals(4, res.size());

        Iterator<DeleteRecordsCommand.Tuple<TopicPartition, Long>> iter = res.iterator();

        DeleteRecordsCommand.Tuple<TopicPartition, Long> t1 = iter.next();
        DeleteRecordsCommand.Tuple<TopicPartition, Long> t2 = iter.next();
        DeleteRecordsCommand.Tuple<TopicPartition, Long> t3 = iter.next();
        DeleteRecordsCommand.Tuple<TopicPartition, Long> t4 = iter.next();

        assertEquals(new TopicPartition("t", 0), t1.v1());
        assertEquals(0L, t1.v2());

        assertEquals(new TopicPartition("t", 1), t2.v1());
        assertEquals(1L, t2.v2());

        assertEquals(t1.v1(), t3.v1());
        assertEquals(2L, t3.v2());

        assertEquals(t1, t4);
    }

    /**
     * Asserts that {@link DeleteRecordsCommand#parseOffsetJsonStringWithoutDedup(String)} throws {@link AdminOperationException}.
     * @param jsonData Data to check.
     */
    private static void assertThrowsAdminOperationException(String jsonData) {
        assertThrows(
            AdminOperationException.class,
            () -> DeleteRecordsCommand.parseOffsetJsonStringWithoutDedup(jsonData)
        );
    }
}