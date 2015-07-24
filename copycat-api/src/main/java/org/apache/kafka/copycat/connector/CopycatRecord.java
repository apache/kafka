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

package org.apache.kafka.copycat.connector;

/**
 * <p>
 * Base class for records containing data to be copied to/from Kafka. This corresponds closely to
 * Kafka's ProducerRecord and ConsumerRecord classes, and holds the data that may be used by both
 * sources and sinks (topic, partition, key, value). Although both implementations include a
 * notion of offest, it is not included here because they differ in type.
 * </p>
 * <p>
 * This class uses type parameters for keys and values. These are provided primarily for
 * connector developer convenience. Internally, Copycat will handle any primitive types or
 * org.apache.kafka.copycat.data types.
 * </p>
 */
public class CopycatRecord {
    private final String topic;
    private final Integer partition;
    private final Object key;
    private final Object value;

    public CopycatRecord(String topic, Integer partition, Object value) {
        this(topic, partition, null, value);
    }

    public CopycatRecord(String topic, Integer partition, Object key, Object value) {
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "CopycatRecord{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", key=" + key +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CopycatRecord that = (CopycatRecord) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        if (partition != null ? !partition.equals(that.partition) : that.partition != null) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }
        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
