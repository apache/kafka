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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.substr;
import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.GROUP_UUID;
import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.GROUP_PARTITION;
import static org.apache.kafka.server.log.remote.storage.RemoteTopicPartitionDirectory.GROUP_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteTopicPartitionDirectoryTest {

    @Test
    public void testSubStr() {
        List<String> topics = Arrays.asList("abcd", "-abcd-10-", "abcd-0-xyz", "abcd.ef-gh-0", "abcd_10_xyz_0");
        for (String topic : topics) {
            for (int i = 0; i < 100; i++) {
                Uuid uuid = Uuid.randomUuid();
                int partition = (int) (Math.random() * 100);
                String filename = String.format("%s-%d-%s", topic, partition, uuid.toString());
                assertEquals(topic, substr(filename, GROUP_TOPIC));
                assertEquals(partition, Integer.parseInt(substr(filename, GROUP_PARTITION)));
                assertEquals(uuid.toString(), substr(filename, GROUP_UUID));
            }
        }
    }
}
