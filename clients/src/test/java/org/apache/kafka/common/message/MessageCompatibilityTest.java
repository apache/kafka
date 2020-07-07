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
package org.apache.kafka.common.message;

import org.apache.kafka.message.MessageSpec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Tests that the messages defined in {@code clients/src/main/resources/common/message/*.json} are compatible across versions of Kafka
 */
@RunWith(Parameterized.class)
public class MessageCompatibilityTest extends AbstractMessageCompatibilityTest {

    /**
     * Filter the result of {@link #messagePairs(String, String)} to remove some versions of some messages.
     *
     * The message json files were added initially and then existing requests/responses were
     * gradually migrated to use the automated format. The migration often involved changing the
     * json in what would be an incompatible way (e.g. renaming fields), but wasn't because
     * the json hasn't been used previously. To cope with this only assert compatibility for versions
     * since the API was migrated to use generate messages.
     *
     * @param messagePairs message pairs.
     * @return The filtered message pairs.
     */
    private static Collection<Object[]> filterNonNormativeVersions(Collection<Object[]> messagePairs) {
        return messagePairs.stream().filter(array -> {
            String messageName = (String) array[0];
            KafkaVersion kafkaVersion = (KafkaVersion) array[1];
            if (messageName.matches("AlterConfigs(Request|Response)")) {
                return kafkaVersion.notBefore("2.6.0");
            } else if (messageName.matches("ApiVersions(Request|Response)")) {
                return kafkaVersion.notBefore("2.4.0");
            } else if (messageName.matches("CreatePartitions(Request|Response)")) {
                return kafkaVersion.notBefore("2.5.0");
            } else if (messageName.matches("CreateTopics(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("DeleteTopics(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("DescribeAcls(Request|Response)")) {
                return kafkaVersion.notBefore("2.5.0");
            } else if (messageName.matches("DescribeConfigs(Request|Response)")) {
                return kafkaVersion.notBefore("2.7.0");
            } else if (messageName.matches("Fetch(Request|Response)")) {
                return kafkaVersion.notBefore("2.7.0");
            } else if (messageName.matches("Heartbeat(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("IncrementalAlterConfigs(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("InitProducerId(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("JoinGroup(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("LeaderAndIsr(Request|Response)")) {
                return kafkaVersion.notBefore("2.4.0");
            } else if (messageName.matches("Metadata(Request|Response)")) {
                return kafkaVersion.notBefore("2.3.0");
            } else if (messageName.matches("OffsetFetch(Request|Response)")) {
                return kafkaVersion.notBefore("2.4.0");
            } else if (messageName.matches("RequestHeader")) {
                return kafkaVersion.notBefore("2.4.0");
            } else if (messageName.matches("StopReplica(Request|Response)")) {
                return kafkaVersion.notBefore("2.4.0");
            } else if (messageName.matches("UpdateMetadata(Request|Response)")) {
                return kafkaVersion.notBefore("2.4.0");
            } else {
                return true;
            }
        }).collect(Collectors.toList());
    }

    @Parameterized.Parameters(name= "{0}: old={1}, new={2}")
    public static Collection<Object[]> parameters() throws IOException {
        return filterNonNormativeVersions(messagePairs("clients/src/main/resources/common/message/", ".json"));
    }

    private final String messageName;
    private final KafkaVersion oldVersion;
    private final MessageSpec oldSpec;
    private final KafkaVersion newVersion;
    private final MessageSpec newSpec;

    public MessageCompatibilityTest(String messageName, KafkaVersion oldVersion, MessageSpec oldSpec, KafkaVersion newVersion, MessageSpec newSpec) {
        this.messageName = messageName;
        this.oldVersion = oldVersion;
        this.oldSpec = oldSpec;
        this.newVersion = newVersion;
        this.newSpec = newSpec;
    }

    @Test
    public void testCompatible() {
        validateCompatibility(messageName, oldVersion, oldSpec, newVersion, newSpec);

        // TODO isn't the default for a string type implicitly null if no explicit default is given?
        // Then it's OK to add a default=null to a string type which was previously null
    }

}
