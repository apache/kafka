package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.consumer.MemberAssignmentImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

public class RangeAssignorV2 implements ConsumerGroupPartitionAssignor {
    @Override
    public String name() {
        return "full-range";
    }

    private static class TopicMetadata {
        public Uuid topicId;
        public int numPartitions;
        public int numMembers;

        public int quota = -1;
        public int extra = -1;
        public int nextRange = 0;

        void maybeComputeQuota() {
            if (quota != -1) return;

            quota = numPartitions / numMembers;
            extra = numPartitions % numMembers;
        }

        @Override
        public String toString() {
            return "TopicMetadata{" +
                "topicId=" + topicId +
                ", numPartitions=" + numPartitions +
                ", numMembers=" + numMembers +
                ", quota=" + quota +
                ", extra=" + extra +
                ", nextRange=" + nextRange +
                '}';
        }
    }

    private GroupAssignment assignHomo(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        List<String> memberIds = new ArrayList<>(groupSpec.memberIds());

        memberIds.sort((memberId1, memberId2) -> {
            Optional<String> instanceId1 = groupSpec.memberSubscription(memberId1).instanceId();
            Optional<String> instanceId2 = groupSpec.memberSubscription(memberId2).instanceId();
            System.out.println("Instance Id1 " + instanceId1 + " and instanceId2 " + instanceId2 );

            if (instanceId1.isPresent() && instanceId2.isPresent()) {
                return instanceId1.get().compareTo(instanceId2.get());
            } else if (instanceId1.isPresent()) {
                return -1;
            } else if (instanceId2.isPresent()) {
                return 1;
            } else {
                return memberId1.compareTo(memberId2);
            }
        });

        MemberSubscription subs = groupSpec.memberSubscription(memberIds.get(0));
        Set<Uuid> subscribedTopics = new HashSet<>(subs.subscribedTopicIds());
        List<TopicMetadata> topics = new ArrayList<>(subscribedTopics.size());
        int numMembers = groupSpec.memberIds().size();

        for (Uuid topicId : subscribedTopics) {
            TopicMetadata m = new TopicMetadata();
            m.topicId = topicId;
            m.numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            m.numMembers = numMembers;
            topics.add(m);
        }

        Map<String, MemberAssignment> assignments = new HashMap<>((int) ((groupSpec.memberIds().size() / 0.75f) + 1));

        for (String memberId : memberIds) {
            Map<Uuid, Set<Integer>> assignment = new HashMap<>((int) ((subscribedTopics.size() / 0.75f) + 1));
            for (TopicMetadata metadata : topics) {
                metadata.maybeComputeQuota();

                if (metadata.nextRange >= metadata.numPartitions) {
                    assignment.put(metadata.topicId, Collections.emptySet());
                } else {
                    int start = metadata.nextRange;
                    int end = Math.min(start + metadata.quota, metadata.numPartitions);
                    if (metadata.extra > 0) {
                        end++;
                        metadata.extra--;
                    }
                    metadata.nextRange = end;
                    assignment.put(metadata.topicId, new RangeSet(start, end));
                }
            }
            assignments.put(memberId, new MemberAssignmentImpl(assignment));
        }
        System.out.println("Final assignment is " + assignments);
        return new GroupAssignment(assignments);
    }

    private GroupAssignment assignHetero(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        System.out.println("Inside hetero code");
        System.out.println("Group Spec " + groupSpec);

        List<String> memberIds = new ArrayList<>(groupSpec.memberIds());

        memberIds.sort((memberId1, memberId2) -> {
            Optional<String> instanceId1 = groupSpec.memberSubscription(memberId1).instanceId();
            Optional<String> instanceId2 = groupSpec.memberSubscription(memberId2).instanceId();

            if (instanceId1.isPresent() && instanceId2.isPresent()) {
                return instanceId1.get().compareTo(instanceId2.get());
            } else if (instanceId1.isPresent()) {
                return -1;
            } else if (instanceId2.isPresent()) {
                return 1;
            } else {
                return memberId1.compareTo(memberId2);
            }
        });

        Map<Uuid, TopicMetadata> topics = new HashMap<>();

        for (String memberId : memberIds) {
            MemberSubscription subs = groupSpec.memberSubscription(memberId);
            for (Uuid topicId : subs.subscribedTopicIds()) {
                TopicMetadata metadata = topics.computeIfAbsent(topicId, __ -> {
                    TopicMetadata m = new TopicMetadata();
                    m.topicId = topicId;
                    m.numPartitions = subscribedTopicDescriber.numPartitions(topicId);
                    return m;
                });
                metadata.numMembers++;
            }
        }
        System.out.println("topics metadata " + topics);
        Map<String, MemberAssignment> assignments = new HashMap<>((int) ((groupSpec.memberIds().size() / 0.75f) + 1));

        for (String memberId : memberIds) {
            MemberSubscription subs = groupSpec.memberSubscription(memberId);
            Map<Uuid, Set<Integer>> assignment = new HashMap<>(subs.subscribedTopicIds().size());
            for (Uuid topicId : subs.subscribedTopicIds()) {
                TopicMetadata metadata = topics.get(topicId);
                metadata.maybeComputeQuota();

                if (metadata.nextRange >= metadata.numPartitions) {
                    assignment.put(metadata.topicId, Collections.emptySet());
                } else {
                    int start = metadata.nextRange;
                    int end = Math.min(start + metadata.quota, metadata.numPartitions);
                    if (metadata.extra > 0) {
                        end++;
                        metadata.extra--;
                    }
                    metadata.nextRange = end;
                    assignment.put(metadata.topicId, new RangeSet(start, end));
                }
            }
            assignments.put(memberId, new MemberAssignmentImpl(assignment));
        }
        System.out.println("Final assignment is " + assignments);
        return new GroupAssignment(assignments);
    }

    private static class RangeSet implements Set<Integer> {
        private final int from;
        private final int to;

        public RangeSet(int from, int to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public int size() {
            return to - from;
        }

        @Override
        public boolean isEmpty() {
            return size() == 0;
        }

        @Override
        public boolean contains(Object o) {
            if (o instanceof Integer) {
                int value = (Integer) o;
                return value >= from && value < to;
            }
            return false;
        }

        @Override
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>() {
                private int current = from;

                @Override
                public boolean hasNext() {
                    return current < to;
                }

                @Override
                public Integer next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    return current++;
                }
            };
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Integer integer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object o : c) {
                if (!contains(o)) return false;
            }
            return true;
        }

        @Override
        public boolean addAll(Collection<? extends Integer> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = from; i < to; i++) {
                sb.append(i);
                if (i < to - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof Set)) return false;

            Set<?> otherSet = (Set<?>) o;
            if (otherSet.size() != this.size()) return false;

            for (int i = from; i < to; i++) {
                if (!otherSet.contains(i)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = from;
            result = 31 * result + to;
            return result;
        }
    }

    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber topicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.subscriptionType() == SubscriptionType.HOMOGENEOUS) {
            return assignHomo(groupSpec, topicDescriber);
        } else {
            return assignHetero(groupSpec, topicDescriber);
        }
    }
}