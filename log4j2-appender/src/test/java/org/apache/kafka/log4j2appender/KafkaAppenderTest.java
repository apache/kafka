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
package org.apache.kafka.log4j2appender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KafkaAppenderTest {

    @Rule
    public LoggerContextRule ctx = new LoggerContextRule("log4j2-appender-test.properties");

    private static Log4jLogEvent createLogEvent(final Level level, final String msg) {
        return Log4jLogEvent.newBuilder()
            .setLoggerName(KafkaAppenderTest.class.getName())
            .setLoggerFqcn(KafkaAppenderTest.class.getName())
            .setLevel(level)
            .setMessage(new SimpleMessage(msg))
            .build();
    }

    private static String getLogMessage(final String topic, final ProducerRecord<byte[], byte[]> record) {
        return new String(Serdes.ByteArray().deserializer().deserialize(topic, record.value()), StandardCharsets.UTF_8);
    }

    @Test
    public void testBrokerList() {
        final KafkaAppender appender = ctx.getAppender("KafkaAppenderWithBrokerList");
        assertThat(appender, notNullValue());

        assertThat(appender.getTopic(), equalTo("test-topic"));
        assertThat(appender.getProducer().getClass(), equalTo(KafkaProducer.class));
        assertThat(appender.getProperties(), notNullValue());
        assertThat(appender.getProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), equalTo("localhost:9092"));
    }

    @Test
    public void testBootstrapServers() {
        final KafkaAppender appender = ctx.getAppender("KafkaAppenderWithBootstrapServers");
        assertThat(appender, notNullValue());

        assertThat(appender.getTopic(), equalTo("test-topic"));
        assertThat(appender.getProducer().getClass(), equalTo(KafkaProducer.class));
        assertThat(appender.getProperties(), notNullValue());
        assertThat(appender.getProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), equalTo("localhost:9092"));
    }

    @Test
    public void testRequiredNumAcks() {
        final KafkaAppender appender = ctx.getAppender("KafkaAppenderWithRequiredNumAcks");
        assertThat(appender, notNullValue());

        assertThat(appender.getTopic(), equalTo("test-topic"));
        assertThat(appender.getProducer().getClass(), equalTo(KafkaProducer.class));
        assertThat(appender.getProperties(), notNullValue());
        assertThat(appender.getProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), equalTo("localhost:9092"));
        assertThat(appender.getProperties().getProperty(ProducerConfig.ACKS_CONFIG), equalTo("all"));
    }

    @Test
    public void testAcks() {
        final KafkaAppender appender = ctx.getAppender("KafkaAppenderWithAcks");
        assertThat(appender, notNullValue());

        assertThat(appender.getTopic(), equalTo("test-topic"));
        assertThat(appender.getProducer().getClass(), equalTo(KafkaProducer.class));
        assertThat(appender.getProperties(), notNullValue());
        assertThat(appender.getProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), equalTo("localhost:9092"));
        assertThat(appender.getProperties().getProperty(ProducerConfig.ACKS_CONFIG), equalTo("all"));
    }

    @Test
    public void testLayoutedMessage() {
        final KafkaAppender appender = ctx.getAppender("KafkaAppenderWithMockProducer");
        assertThat(appender, notNullValue());

        assertThat(appender.getTopic(), equalTo("test-topic"));
        assertThat(appender.getProducer().getClass(), equalTo(DummyKafkaProducer.class));
        assertThat(appender.getProperties(), notNullValue());
        assertThat(appender.getProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), equalTo("localhost:9092"));

        appender.append(createLogEvent(Level.INFO, "info message"));
        appender.append(createLogEvent(Level.WARN, "warning message"));
        appender.append(createLogEvent(Level.ERROR, "error message"));

        final MockProducer<byte[], byte[]> producer = (MockProducer<byte[], byte[]>) appender.getProducer();
        assertThat(getLogMessage("test-topic", producer.history().get(0)), equalTo("[INFO] info message"));
        assertThat(getLogMessage("test-topic", producer.history().get(1)), equalTo("[WARN] warning message"));
        assertThat(getLogMessage("test-topic", producer.history().get(2)), equalTo("[ERROR] error message"));
    }
}
