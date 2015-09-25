/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.copycat.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.copycat.connector.Task;

import java.util.Collection;
import java.util.Map;

/**
 * SinkTask is a Task takes records loaded from Kafka and sends them to another system. In
 * addition to the basic {@link #put} interface, SinkTasks must also implement {@link #flush}
 * to support offset commits.
 */
@InterfaceStability.Unstable
public abstract class SinkTask implements Task {

    /**
     * <p>
     * The configuration key that provides the list of topics that are inputs for this
     * SinkTask.
     * </p>
     */
    public static final String TOPICS_CONFIG = "topics";

    protected SinkTaskContext context;

    public void initialize(SinkTaskContext context) {
        this.context = context;
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * @param records the set of records to send
     */
    public abstract void put(Collection<SinkRecord> records);

    /**
     * Flush all records that have been {@link #put} for the specified topic-partitions. The
     * offsets are provided for convenience, but could also be determined by tracking all offsets
     * included in the SinkRecords passed to {@link #put}.
     *
     * @param offsets mapping of TopicPartition to committed offset
     */
    public abstract void flush(Map<TopicPartition, OffsetAndMetadata> offsets);
}
