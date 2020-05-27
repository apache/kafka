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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ApplicableTransformationTest {

    private static final UUID ORIGINAL_VALUE = UUID.randomUUID();
    private static final UUID TRANSFORMED_VALUE = UUID.randomUUID();
    private static final String INCLUDED_TOPIC_NAME = "includedTopic";
    private static final String EXCLUDED_TOPIC_NAME = "excludedTopic";
    private static final SinkRecord INCLUDED_TOPIC_RECORD = new SinkRecord(INCLUDED_TOPIC_NAME, 0, null, null, null, ORIGINAL_VALUE, 0);
    private static final SinkRecord EXCLUDED_TOPIC_RECORD = new SinkRecord(EXCLUDED_TOPIC_NAME, 0, null, null, null, ORIGINAL_VALUE, 0);

    @Test
    public void itShouldRunTransformationOnIncludedTopicRecord() {
        final TestTransformation<SinkRecord> xform = new TestTransformation<>();

        final Map<String, Object> config = buildConfigWithIncludedTopic(INCLUDED_TOPIC_NAME);
        xform.configure(config);
        final ApplicableTransformation<SinkRecord> applicableTransformation = new ApplicableTransformation<>(
                xform, config);

        final SinkRecord transformedRecord = applicableTransformation.apply(INCLUDED_TOPIC_RECORD);
        assertEquals(TRANSFORMED_VALUE, transformedRecord.value());
    }

    @Test
    public void itShouldNotRunTransformationOnExcludedTopicRecord() {
        final TestTransformation<SinkRecord> xform = new TestTransformation<>();

        final Map<String, Object> config = buildConfigWithIncludedTopic(INCLUDED_TOPIC_NAME);
        xform.configure(config);
        final ApplicableTransformation<SinkRecord> applicableTransformation = new ApplicableTransformation<>(
                xform, config);

        final SinkRecord transformedRecord = applicableTransformation.apply(EXCLUDED_TOPIC_RECORD);
        assertEquals(ORIGINAL_VALUE, transformedRecord.value());
    }

    @Test
    public void itShouldRunAnyTransformationWithWhenTopicsIsNotDefined() {
        final TestTransformation<SinkRecord> xform = new TestTransformation<>();

        final Map<String, Object> config = buildConfig();
        xform.configure(config);
        final ApplicableTransformation<SinkRecord> applicableTransformation = new ApplicableTransformation<>(
                xform, config);

        assertEquals(ORIGINAL_VALUE, applicableTransformation.apply(INCLUDED_TOPIC_RECORD).value());
        assertEquals(ORIGINAL_VALUE, applicableTransformation.apply(EXCLUDED_TOPIC_RECORD).value());
    }

    private static Map<String, Object> buildConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put("value", TRANSFORMED_VALUE);
        return config;
    }

    private static Map<String, Object> buildConfigWithIncludedTopic(final String topicsConfig) {
        final Map<String, Object> config = buildConfig();
        config.put("topics", topicsConfig);
        return config;
    }

    public static class TestTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

        UUID value;

        @Override
        public R apply(R record) {
            return record
                    .newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                            record.valueSchema(), value, record.timestamp());
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("value", Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "");
        }

        @Override
        public void configure(final Map<String, ?> props) {
            value = (UUID) props.get("value");
        }

        @Override
        public void close() {
        }
    }

}