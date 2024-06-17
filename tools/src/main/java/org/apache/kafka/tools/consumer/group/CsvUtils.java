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
package org.apache.kafka.tools.consumer.group;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class CsvUtils {
    private final static CsvMapper MAPPER = new CsvMapper();

    static ObjectReader readerFor(Class<?> clazz) {
        return MAPPER.readerFor(clazz).with(getSchema(clazz));
    }

    static ObjectWriter writerFor(Class<?> clazz) {
        return MAPPER.writerFor(clazz).with(getSchema(clazz));
    }

    private static CsvSchema getSchema(Class<?> clazz) {
        String[] fields;
        if (CsvRecordWithGroup.class == clazz)
            fields = CsvRecordWithGroup.FIELDS;
        else if (CsvRecordNoGroup.class == clazz)
            fields = CsvRecordNoGroup.FIELDS;
        else
            throw new IllegalStateException("Unhandled class " + clazz);

        return MAPPER.schemaFor(clazz).sortedBy(fields);
    }

    public static class CsvRecordWithGroup {
        public static final String[] FIELDS = new String[] {"group", "topic", "partition", "offset"};

        @JsonProperty
        private String group;

        @JsonProperty
        private String topic;

        @JsonProperty
        private int partition;

        @JsonProperty
        private long offset;

        /**
         * Required for jackson.
         */
        public CsvRecordWithGroup() {
        }

        public CsvRecordWithGroup(String group, String topic, int partition, long offset) {
            this.group = group;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getGroup() {
            return group;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }
    }

    public static class CsvRecordNoGroup {
        public static final String[] FIELDS = new String[]{"topic", "partition", "offset"};

        @JsonProperty
        private String topic;

        @JsonProperty
        private int partition;

        @JsonProperty
        private long offset;

        /**
         * Required for jackson.
         */
        public CsvRecordNoGroup() {
        }

        public CsvRecordNoGroup(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }
    }
}
