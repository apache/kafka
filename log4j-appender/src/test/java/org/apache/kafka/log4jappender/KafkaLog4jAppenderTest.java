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
package org.apache.kafka.log4jappender;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.helpers.LogLog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaLog4jAppenderTest {

    private Logger logger = Logger.getLogger(KafkaLog4jAppenderTest.class);

    @Before
    public void setup() {
        LogLog.setInternalDebugging(true);
    }

    @Test
    public void testKafkaLog4jConfigs() {
        // host missing
        Properties props = new Properties();
        props.put("log4j.rootLogger", "INFO");
        props.put("log4j.appender.KAFKA", "org.apache.kafka.log4jappender.KafkaLog4jAppender");
        props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n");
        props.put("log4j.appender.KAFKA.Topic", "test-topic");
        props.put("log4j.logger.kafka.log4j", "INFO, KAFKA");

        try {
            PropertyConfigurator.configure(props);
            Assert.fail("Missing properties exception was expected !");
        } catch (ConfigException ex) {
            // It's OK!
        }

        // topic missing
        props = new Properties();
        props.put("log4j.rootLogger", "INFO");
        props.put("log4j.appender.KAFKA", "org.apache.kafka.log4jappender.KafkaLog4jAppender");
        props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n");
        props.put("log4j.appender.KAFKA.brokerList", "127.0.0.1:9093");
        props.put("log4j.logger.kafka.log4j", "INFO, KAFKA");

        try {
            PropertyConfigurator.configure(props);
            Assert.fail("Missing properties exception was expected !");
        } catch (ConfigException ex) {
            // It's OK!
        }
    }


    @Test
    public void testLog4jAppends() {
        PropertyConfigurator.configure(getLog4jConfig(false));

        for (int i = 1; i <= 5; ++i) {
            logger.error(getMessage(i));
        }

        Assert.assertEquals(
                5, (getMockkafkaLog4jAppender()).getHistory().size());
    }

    @Test(expected = RuntimeException.class)
    public void testLog4jAppendsWithSyncSendAndSimulateProducerFailShouldThrowException() {
        Properties props = getLog4jConfig(true);
        props.put("log4j.appender.KAFKA.IgnoreExceptions", "false");
        PropertyConfigurator.configure(props);

        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockkafkaLog4jAppender();
        generateMockProducerResponse(mockKafkaLog4jAppender, true);
        logger.error(getMessage(0));
    }

    @Test
    public void testLog4jAppendsWithSyncSendWithoutIgnoringExceptionsShouldNotThrowException() {
        Properties props = getLog4jConfig(true);
        props.put("log4j.appender.KAFKA.IgnoreExceptions", "false");
        PropertyConfigurator.configure(props);

        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockkafkaLog4jAppender();
        generateMockProducerResponse(mockKafkaLog4jAppender, false);
        logger.error(getMessage(0));

        Assert.assertEquals(
                1, mockKafkaLog4jAppender.getHistory().size());
    }

    @Test
    public void testLog4jAppendsWithRealProducerConfigWithSyncSendShouldNotThrowException() {
        Properties props = getLog4jConfigWithRealProducer(true);
        PropertyConfigurator.configure(props);

        logger.error(getMessage(0));
    }

    private MockKafkaLog4jAppender getMockkafkaLog4jAppender() {
        return (MockKafkaLog4jAppender) Logger.getRootLogger().getAppender("KAFKA");
    }

    private void generateMockProducerResponse(final MockKafkaLog4jAppender mockKafkaLog4jAppender,
                                              final boolean error) {
        MockProducer<byte[], byte[]> kafkaProducer =
                (MockProducer<byte[], byte[]>) mockKafkaLog4jAppender.getKafkaProducer(null);
        new Thread(() -> {
            try {
                Thread.sleep(300);
                if (error)
                    kafkaProducer.errorNext(new TimeoutException());
                else
                    kafkaProducer.completeNext();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Test(expected = RuntimeException.class)
    public void testLog4jAppendsWithRealProducerConfigWithSyncSendAndNotIgnoringExceptionsShouldThrowException() {
        Properties props = getLog4jConfigWithRealProducer(false);
        PropertyConfigurator.configure(props);

        logger.error(getMessage(0));
    }

    private byte[] getMessage(int i) {
        return ("test_" + i).getBytes(StandardCharsets.UTF_8);
    }

    private Properties getLog4jConfigWithRealProducer(boolean ignoreExceptions) {
        Properties props = new Properties();
        props.put("log4j.rootLogger", "INFO, KAFKA");
        props.put("log4j.appender.KAFKA", "org.apache.kafka.log4jappender.KafkaLog4jAppender");
        props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n");
        props.put("log4j.appender.KAFKA.BrokerList", "127.0.0.2:9093");
        props.put("log4j.appender.KAFKA.Topic", "test-topic");
        props.put("log4j.appender.KAFKA.RequiredNumAcks", "1");
        props.put("log4j.appender.KAFKA.SyncSend", "true");
        // setting producer timeout to be low
        props.put("log4j.appender.KAFKA.timeout", "10");
        // ignoring exceptions
        props.put("log4j.appender.KAFKA.IgnoreExceptions", Boolean.toString(ignoreExceptions));
        props.put("log4j.logger.kafka.log4j", "INFO, KAFKA");
        return props;
    }

    private Properties getLog4jConfig(boolean syncSend) {
        Properties props = new Properties();
        props.put("log4j.rootLogger", "INFO, KAFKA");
        props.put("log4j.appender.KAFKA", "org.apache.kafka.log4jappender.MockKafkaLog4jAppender");
        props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout");
        props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n");
        props.put("log4j.appender.KAFKA.BrokerList", "127.0.0.1:9093");
        props.put("log4j.appender.KAFKA.Topic", "test-topic");
        props.put("log4j.appender.KAFKA.RequiredNumAcks", "1");
        props.put("log4j.appender.KAFKA.SyncSend", Boolean.toString(syncSend));
        props.put("log4j.logger.kafka.log4j", "INFO, KAFKA");
        return props;
    }
}

