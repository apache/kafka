/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.sink;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

/**
 * SinkRecord is a {@link ConnectRecord} that has been read from Kafka and includes the kafkaOffset of
 * the record in the Kafka topic-partition in addition to the standard fields. This information
 * should be used by the SinkTask to coordinate kafkaOffset commits.
 */
@InterfaceStability.Unstable
public class SinkRecord extends ConnectRecord {
    private final long kafkaOffset;

    public SinkRecord(String topic, int partition, Schema keySchema, Object key, Schema valueSchema, Object value, long kafkaOffset) {
        super(topic, partition, keySchema, key, valueSchema, value);
        this.kafkaOffset = kafkaOffset;
    }

    public long kafkaOffset() {
        return kafkaOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        SinkRecord that = (SinkRecord) o;

        if (kafkaOffset != that.kafkaOffset)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (kafkaOffset ^ (kafkaOffset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "kafkaOffset=" + kafkaOffset +
                "} " + super.toString();
    }
}
