package org.apache.kafka.server.util;

import java.util.Set;

public interface PartitionFilter {
    /**
     * Used to filter partitions based on a certain criteria, for example, a set of partition ids.
     */
    public boolean isPartitionAllowed(int partition);

    public static class PartitionsSetFilter implements PartitionFilter {
        private final Set<Integer> partitionIds;
        public PartitionsSetFilter(Set<Integer> partitionIds) {
            this.partitionIds = partitionIds;
        }
        @Override
        public boolean isPartitionAllowed(int partition) {
            return partitionIds.isEmpty() || partitionIds.contains(partition);
        }
    }

    class UniquePartitionFilter implements PartitionFilter {
        private final int partition;

        public UniquePartitionFilter(int partition) {
            this.partition = partition;
        }

        @Override
        public boolean isPartitionAllowed(int partition) {
            return partition == this.partition;
        }
    }

    class PartitionRangeFilter implements PartitionFilter {
        private final int lowerRange;
        private final int upperRange;

        public PartitionRangeFilter(int lowerRange, int upperRange) {
            this.lowerRange = lowerRange;
            this.upperRange = upperRange;
        }

        @Override
        public boolean isPartitionAllowed(int partition) {
            return partition >= lowerRange && partition < upperRange;
        }
    }
}
