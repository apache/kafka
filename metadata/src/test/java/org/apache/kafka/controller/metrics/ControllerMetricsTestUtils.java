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

package org.apache.kafka.controller.metrics;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ControllerMetricsTestUtils {
    public static void assertMetricsForTypeEqual(
        MetricsRegistry registry,
        String expectedPrefix,
        Set<String> expected
    ) {
        Set<String> actual = new TreeSet<>();
        registry.allMetrics().forEach((name, __) -> {
            StringBuilder bld = new StringBuilder();
            bld.append(name.getGroup());
            bld.append(":type=").append(name.getType());
            bld.append(",name=").append(name.getName());
            if (bld.toString().startsWith(expectedPrefix)) {
                actual.add(bld.toString());
            }
        });
        assertEquals(new TreeSet<>(expected), actual);
    }

    enum FakePartitionRegistrationType {
        NORMAL,
        NON_PREFERRED_LEADER,
        OFFLINE
    }

    public static PartitionRegistration fakePartitionRegistration(
        FakePartitionRegistrationType  type
    ) {
        int leader = 0;
        switch (type) {
            case NORMAL:
                leader = 0;
                break;
            case NON_PREFERRED_LEADER:
                leader = 1;
                break;
            case OFFLINE:
                leader = -1;
                break;
        }
        return new PartitionRegistration.Builder().
            setReplicas(new int[] {0, 1, 2}).
            setIsr(new int[] {0, 1, 2}).
            setLeader(leader).
            setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
            setLeaderEpoch(100).
            setPartitionEpoch(200).
            build();
    }

    public static TopicImage fakeTopicImage(
        String topicName,
        Uuid topicId,
        PartitionRegistration... registrations
    ) {
        Map<Integer, PartitionRegistration> partitions = new HashMap<>();
        int i = 0;
        for (PartitionRegistration registration : registrations) {
            partitions.put(i, registration);
            i++;
        }
        return new TopicImage(topicName, topicId, partitions);
    }

    public static TopicsImage fakeTopicsImage(
        TopicImage... topics
    ) {
        TopicsImage image = TopicsImage.EMPTY;
        for (TopicImage topic : topics) {
            image = image.including(topic);
        }
        return image;
    }
}
