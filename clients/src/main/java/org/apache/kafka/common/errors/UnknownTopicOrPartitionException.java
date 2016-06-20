/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.errors;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * This topic/partition doesn't exist
 */
public class UnknownTopicOrPartitionException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public UnknownTopicOrPartitionException(final String message) {
        super(message);
    }

    public UnknownTopicOrPartitionException(TopicPartition unknownTopicOrPartition) {
        this(new HashSet<TopicPartition>(Arrays.asList(unknownTopicOrPartition)));
    }

    public UnknownTopicOrPartitionException(Set<TopicPartition> unknownTopicOrPartitions) {
        this(unknownTopicOrPartitions, null);
    }

    public UnknownTopicOrPartitionException(Set<TopicPartition> unknownTopicOrPartitions, Throwable throwable) {
        super(org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION.message() + unknownTopicOrPartitions.toString(), throwable);
    }

}
