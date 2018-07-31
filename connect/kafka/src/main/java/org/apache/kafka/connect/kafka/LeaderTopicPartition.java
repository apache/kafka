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
package org.apache.kafka.connect.kafka;

import org.apache.kafka.common.TopicPartition;

public class LeaderTopicPartition extends Object {

    private static final String STRING_DELIMITER = ":";

    private int hash = 0;

    private final int leaderId;
    private final String topicName;
    private final int partition;

    public LeaderTopicPartition(int leaderId, String topicName, int partition) throws IllegalArgumentException {
        this.leaderId = leaderId;
        if (topicName == null)
            throw new IllegalArgumentException("topicName can not be null");
        this.topicName = topicName;
        this.partition = partition;
    }

    public static LeaderTopicPartition fromString(String leaderTopicPartitionString) {
        String[] tokens = leaderTopicPartitionString.split(STRING_DELIMITER);
        if (tokens.length != 3)
            throw new IllegalArgumentException("leaderTopicPartitionString must be in the format <leader>:<topic>:<partition>");
        return new LeaderTopicPartition(Integer.parseInt(tokens[0], 10), tokens[1], Integer.parseInt(tokens[2], 10));
    }

    @Override
    public String toString() {
        return String.valueOf(leaderId) +
                STRING_DELIMITER +
                topicName +
                STRING_DELIMITER +
                String.valueOf(partition);
    }

    public TopicPartition toTopicPartition() {
        return new TopicPartition(topicName, partition);
    }

    public String toTopicPartitionString() {
        return topicName + STRING_DELIMITER + String.valueOf(partition);
    }


    @Override
    public int hashCode() {
        if (hash != 0)
            return hash;
        int result = 1;
        result = result * 23 + leaderId;
        result = result * 37 + (topicName == null ? 0 : topicName.hashCode());
        result = result * 11 + partition;
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (!(other instanceof LeaderTopicPartition))
            return false;
        LeaderTopicPartition otherLeaderTopicPartition = (LeaderTopicPartition) other;
        return leaderId == otherLeaderTopicPartition.leaderId
                && ((topicName == null) ? otherLeaderTopicPartition.topicName == null : topicName.equals(otherLeaderTopicPartition.topicName))
                && partition == otherLeaderTopicPartition.partition;
    }

}
