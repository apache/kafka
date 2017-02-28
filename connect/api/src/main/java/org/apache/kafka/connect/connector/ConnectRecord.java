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

package org.apache.kafka.connect.connector;

import org.apache.kafka.connect.data.Schema;

/**
 * <p>
 * Base class for records containing data to be copied to/from Kafka. This corresponds closely to
 * Kafka's ProducerRecord and ConsumerRecord classes, and holds the data that may be used by both
 * sources and sinks (topic, kafkaPartition, key, value). Although both implementations include a
 * notion of offset, it is not included here because they differ in type.
 * </p>
 */
public abstract class ConnectRecord<R extends ConnectRecord<R>> {
    private final String topic;
    private final Integer kafkaPartition;
    private final Schema keySchema;
    private final Object key;
    private final Schema valueSchema;
    private final Object value;
    private final Long timestamp;

    public ConnectRecord(String topic, Integer kafkaPartition,
                         Schema keySchema, Object key,
                         Schema valueSchema, Object value,
                         Long timestamp) {
        this.topic = topic;
        this.kafkaPartition = kafkaPartition;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String topic() {
        return topic;
    }

    public Integer kafkaPartition() {
        return kafkaPartition;
    }

    public Object key() {
        return key;
    }

    public Schema keySchema() {
        return keySchema;
    }

    public Object value() {
        return value;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    public Long timestamp() {
        return timestamp;
    }

    /** Generate a new record of the same type as itself, with the specified parameter values. **/
    public abstract R newRecord(String topic, Integer kafkaPartition, Schema keySchema, Object key, Schema valueSchema, Object value, Long timestamp);

    @Override
    public String toString() {
        return "ConnectRecord{" +
                "topic='" + topic + '\'' +
                ", kafkaPartition=" + kafkaPartition +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ConnectRecord that = (ConnectRecord) o;

        if (kafkaPartition != null ? !kafkaPartition.equals(that.kafkaPartition) : that.kafkaPartition != null)
            return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null)
            return false;
        if (keySchema != null ? !keySchema.equals(that.keySchema) : that.keySchema != null)
            return false;
        if (key != null ? !key.equals(that.key) : that.key != null)
            return false;
        if (valueSchema != null ? !valueSchema.equals(that.valueSchema) : that.valueSchema != null)
            return false;
        if (value != null ? !value.equals(that.value) : that.value != null)
            return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (kafkaPartition != null ? kafkaPartition.hashCode() : 0);
        result = 31 * result + (keySchema != null ? keySchema.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (valueSchema != null ? valueSchema.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
