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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StickyAssignorUserData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * <p>The sticky assignor serves two purposes. First, it guarantees an assignment that is as balanced as possible, meaning either:
 * <ul>
 * <li>the numbers of topic partitions assigned to consumers differ by at most one; or</li>
 * <li>each consumer that has 2+ fewer topic partitions than some other consumer cannot get any of those topic partitions transferred to it.</li>
 * </ul>
 * Second, it preserved as many existing assignment as possible when a reassignment occurs. This helps in saving some of the
 * overhead processing when topic partitions move from one consumer to another.</p>
 *
 * <p>Starting fresh it would work by distributing the partitions over consumers as evenly as possible. Even though this may sound similar to
 * how round robin assignor works, the second example below shows that it is not.
 * During a reassignment it would perform the reassignment in such a way that in the new assignment
 * <ol>
 * <li>topic partitions are still distributed as evenly as possible, and</li>
 * <li>topic partitions stay with their previously assigned consumers as much as possible.</li>
 * </ol>
 * Of course, the first goal above takes precedence over the second one.</p>
 *
 * <p><b>Example 1.</b> Suppose there are three consumers <code>C0</code>, <code>C1</code>, <code>C2</code>,
 * four topics <code>t0,</code> <code>t1</code>, <code>t2</code>, <code>t3</code>, and each topic has 2 partitions,
 * resulting in partitions <code>t0p0</code>, <code>t0p1</code>, <code>t1p0</code>, <code>t1p1</code>, <code>t2p0</code>,
 * <code>t2p1</code>, <code>t3p0</code>, <code>t3p1</code>. Each consumer is subscribed to all three topics.
 *
 * The assignment with both sticky and round robin assignors will be:
 * <ul>
 * <li><code>C0: [t0p0, t1p1, t3p0]</code></li>
 * <li><code>C1: [t0p1, t2p0, t3p1]</code></li>
 * <li><code>C2: [t1p0, t2p1]</code></li>
 * </ul>
 *
 * Now, let's assume <code>C1</code> is removed and a reassignment is about to happen. The round robin assignor would produce:
 * <ul>
 * <li><code>C0: [t0p0, t1p0, t2p0, t3p0]</code></li>
 * <li><code>C2: [t0p1, t1p1, t2p1, t3p1]</code></li>
 * </ul>
 *
 * while the sticky assignor would result in:
 * <ul>
 * <li><code>C0 [t0p0, t1p1, t3p0, t2p0]</code></li>
 * <li><code>C2 [t1p0, t2p1, t0p1, t3p1]</code></li>
 * </ul>
 * preserving all the previous assignments (unlike the round robin assignor).
 *</p>
 * <p><b>Example 2.</b> There are three consumers <code>C0</code>, <code>C1</code>, <code>C2</code>,
 * and three topics <code>t0</code>, <code>t1</code>, <code>t2</code>, with 1, 2, and 3 partitions respectively.
 * Therefore, the partitions are <code>t0p0</code>, <code>t1p0</code>, <code>t1p1</code>, <code>t2p0</code>,
 * <code>t2p1</code>, <code>t2p2</code>. <code>C0</code> is subscribed to <code>t0</code>; <code>C1</code> is subscribed to
 * <code>t0</code>, <code>t1</code>; and <code>C2</code> is subscribed to <code>t0</code>, <code>t1</code>, <code>t2</code>.
 *
 * The round robin assignor would come up with the following assignment:
 * <ul>
 * <li><code>C0 [t0p0]</code></li>
 * <li><code>C1 [t1p0]</code></li>
 * <li><code>C2 [t1p1, t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * which is not as balanced as the assignment suggested by sticky assignor:
 * <ul>
 * <li><code>C0 [t0p0]</code></li>
 * <li><code>C1 [t1p0, t1p1]</code></li>
 * <li><code>C2 [t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * Now, if consumer <code>C0</code> is removed, these two assignors would produce the following assignments.
 * Round Robin (preserves 3 partition assignments):
 * <ul>
 * <li><code>C1 [t0p0, t1p1]</code></li>
 * <li><code>C2 [t1p0, t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *
 * Sticky (preserves 5 partition assignments):
 * <ul>
 * <li><code>C1 [t1p0, t1p1, t0p0]</code></li>
 * <li><code>C2 [t2p0, t2p1, t2p2]</code></li>
 * </ul>
 *</p>
 * <h3>Impact on <code>ConsumerRebalanceListener</code></h3>
 * The sticky assignment strategy can provide some optimization to those consumers that have some partition cleanup code
 * in their <code>onPartitionsRevoked()</code> callback listeners. The cleanup code is placed in that callback listener
 * because the consumer has no assumption or hope of preserving any of its assigned partitions after a rebalance when it
 * is using range or round robin assignor. The listener code would look like this:
 * <pre>
 * {@code
 * class TheOldRebalanceListener implements ConsumerRebalanceListener {
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       commitOffsets(partition);
 *       cleanupState(partition);
 *     }
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions) {
 *       initializeState(partition);
 *       initializeOffset(partition);
 *     }
 *   }
 * }
 * }
 * </pre>
 *
 * As mentioned above, one advantage of the sticky assignor is that, in general, it reduces the number of partitions that
 * actually move from one consumer to another during a reassignment. Therefore, it allows consumers to do their cleanup
 * more efficiently. Of course, they still can perform the partition cleanup in the <code>onPartitionsRevoked()</code>
 * listener, but they can be more efficient and make a note of their partitions before and after the rebalance, and do the
 * cleanup after the rebalance only on the partitions they have lost (which is normally not a lot). The code snippet below
 * clarifies this point:
 * <pre>
 * {@code
 * class TheNewRebalanceListener implements ConsumerRebalanceListener {
 *   Collection<TopicPartition> lastAssignment = Collections.emptyList();
 *
 *   void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *     for (TopicPartition partition: partitions)
 *       commitOffsets(partition);
 *   }
 *
 *   void onPartitionsAssigned(Collection<TopicPartition> assignment) {
 *     for (TopicPartition partition: difference(lastAssignment, assignment))
 *       cleanupState(partition);
 *
 *     for (TopicPartition partition: difference(assignment, lastAssignment))
 *       initializeState(partition);
 *
 *     for (TopicPartition partition: assignment)
 *       initializeOffset(partition);
 *
 *     this.lastAssignment = assignment;
 *   }
 * }
 * }
 * </pre>
 *
 * Any consumer that uses sticky assignment can leverage this listener like this:
 * <code>consumer.subscribe(topics, new TheNewRebalanceListener());</code>
 *
 * Note that you can leverage the {@link CooperativeStickyAssignor} so that only partitions which are being
 * reassigned to another consumer will be revoked. That is the preferred assignor for newer cluster. See
 * {@link ConsumerPartitionAssignor.RebalanceProtocol} for a detailed explanation of cooperative rebalancing.
 */
public class StickyAssignor extends AbstractStickyAssignor {

    private List<TopicPartition> memberAssignment = null;
    private int generation = DEFAULT_GENERATION; // consumer group generation

    @Override
    public String name() {
        return "sticky";
    }

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        memberAssignment = assignment.partitions();
        this.generation = metadata.generationId();
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        if (memberAssignment == null)
            return null;

        return serializeTopicPartitionAssignment(new MemberData(memberAssignment, Optional.of(generation)));
    }

    @Override
    protected MemberData memberData(Subscription subscription) {
        ByteBuffer userData = subscription.userData();
        if (userData == null || !userData.hasRemaining()) {
            return new MemberData(Collections.emptyList(), Optional.empty());
        }
        return deserializeTopicPartitionAssignment(userData);
    }

    // visible for testing
    static ByteBuffer serializeTopicPartitionAssignment(MemberData memberData) {
        return serializeTopicPartitionAssignment(memberData, StickyAssignorUserData.HIGHEST_SUPPORTED_VERSION);
    }

    // visible for testing
    static ByteBuffer serializeTopicPartitionAssignment(MemberData memberData, short version) {

        StickyAssignorUserData.TopicPartitionsCollection topicAssignments = new StickyAssignorUserData.TopicPartitionsCollection();
        if (memberData.partitions != null) {
            memberData.partitions.forEach(partition -> {
                StickyAssignorUserData.TopicPartitions topicPartitions = topicAssignments.find(partition.topic());
                if (topicPartitions == null) {
                    topicPartitions = new StickyAssignorUserData.TopicPartitions()
                        .setTopic(partition.topic());
                    topicAssignments.add(topicPartitions);
                }
                topicPartitions.partitions().add(partition.partition());
            });
        }
        StickyAssignorUserData data = new StickyAssignorUserData()
                .setPreviousAssignment(topicAssignments);
        memberData.generation.ifPresent(data::setGeneration);
        return MessageUtil.toByteBuffer(data, version);
    }

    private static MemberData deserializeTopicPartitionAssignment(ByteBuffer buffer, short version) {
        StickyAssignorUserData data = new StickyAssignorUserData(new ByteBufferAccessor(buffer), version);

        List<TopicPartition> partitions = new ArrayList<>();
        for (StickyAssignorUserData.TopicPartitions topicPartition : data.previousAssignment()) {
            String topic = topicPartition.topic();
            for (Integer partition : topicPartition.partitions()) {
                partitions.add(new TopicPartition(topic, partition));
            }
        }
        // make sure this is backward compatible
        Optional<Integer> generation = data.generation() > 0 ? Optional.of(data.generation()) : Optional.empty();
        return new MemberData(partitions, generation);
    }

    // visible for testing
    static MemberData deserializeTopicPartitionAssignment(ByteBuffer buffer) {

        for (short version = StickyAssignorUserData.HIGHEST_SUPPORTED_VERSION;
             version >= StickyAssignorUserData.LOWEST_SUPPORTED_VERSION; version--) {
            ByteBuffer copy = buffer.duplicate();
            try {
                return deserializeTopicPartitionAssignment(buffer, version);
            } catch (Exception e) {
                // fall back to older schema
                buffer = copy;
            }
        }
        // ignore the consumer's previous assignment if it cannot be parsed
        return new MemberData(Collections.emptyList(), Optional.of(DEFAULT_GENERATION));
    }
}
