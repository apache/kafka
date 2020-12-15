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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.helpers.LogLog;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

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
    public void testSetSaslMechanism() {
        Properties props = getLog4jConfig(false);
        props.put("log4j.appender.KAFKA.SaslMechanism", "PLAIN");
        PropertyConfigurator.configure(props);

        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockKafkaLog4jAppender();
        assertThat(
                mockKafkaLog4jAppender.getProducerProperties().getProperty(SaslConfigs.SASL_MECHANISM),
                equalTo("PLAIN"));
    }

    @Test
    public void testSaslMechanismNotSet() {
        testProducerPropertyNotSet(SaslConfigs.SASL_MECHANISM);
    }

    @Test
    public void testSetJaasConfig() {
        Properties props = getLog4jConfig(false);
        props.put("log4j.appender.KAFKA.ClientJaasConf", "jaas-config");
        PropertyConfigurator.configure(props);

        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockKafkaLog4jAppender();
        assertThat(
                mockKafkaLog4jAppender.getProducerProperties().getProperty(SaslConfigs.SASL_JAAS_CONFIG),
                equalTo("jaas-config"));
    }

    @Test
    public void testJaasConfigNotSet() {
        testProducerPropertyNotSet(SaslConfigs.SASL_JAAS_CONFIG);
    }

    private void testProducerPropertyNotSet(String name) {
        PropertyConfigurator.configure(getLog4jConfig(false));
        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockKafkaLog4jAppender();
        assertThat(mockKafkaLog4jAppender.getProducerProperties().stringPropertyNames(), not(hasItem(name)));
    }

    @Test
    public void testLog4jAppends() {
        PropertyConfigurator.configure(getLog4jConfig(false));

        for (int i = 1; i <= 5; ++i) {
            logger.error(getMessage(i));
        }

        Assert.assertEquals(
            5, (getMockKafkaLog4jAppender()).getHistory().size());
    }

    @Test(expected = RuntimeException.class)
    public void testLog4jAppendsWithSyncSendAndSimulateProducerFailShouldThrowException() {
        Properties props = getLog4jConfig(true);
        props.put("log4j.appender.KAFKA.IgnoreExceptions", "false");
        PropertyConfigurator.configure(props);

        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockKafkaLog4jAppender();
        replaceProducerWithMocked(mockKafkaLog4jAppender, false);

        logger.error(getMessage(0));
    }

    @Test
    public void testLog4jAppendsWithSyncSendWithoutIgnoringExceptionsShouldNotThrowException() {
        Properties props = getLog4jConfig(true);
        props.put("log4j.appender.KAFKA.IgnoreExceptions", "false");
        PropertyConfigurator.configure(props);

        MockKafkaLog4jAppender mockKafkaLog4jAppender = getMockKafkaLog4jAppender();
        replaceProducerWithMocked(mockKafkaLog4jAppender, true);

        logger.error(getMessage(0));
    }

    @Test
    public void testLog4jAppendsWithRealProducerConfigWithSyncSendShouldNotThrowException() {
        Properties props = getLog4jConfigWithRealProducer(true);
        PropertyConfigurator.configure(props);

        logger.error(getMessage(0));
    }

    @Test(expected = RuntimeException.class)
    public void testLog4jAppendsWithRealProducerConfigWithSyncSendAndNotIgnoringExceptionsShouldThrowException() {
        Properties props = getLog4jConfigWithRealProducer(false);
        PropertyConfigurator.configure(props);

        logger.error(getMessage(0));
    }

    private void replaceProducerWithMocked(MockKafkaLog4jAppender mockKafkaLog4jAppender, boolean success) {
        @SuppressWarnings("unchecked")
        MockProducer<byte[], byte[]> producer = EasyMock.niceMock(MockProducer.class);
        @SuppressWarnings("unchecked")
        Future<RecordMetadata> futureMock = EasyMock.niceMock(Future.class);
        try {
            if (!success)
                EasyMock.expect(futureMock.get())
                    .andThrow(new ExecutionException("simulated timeout", new TimeoutException()));
        } catch (InterruptedException | ExecutionException e) {
            // just mocking
        }
        EasyMock.expect(producer.send(EasyMock.anyObject())).andReturn(futureMock);
        EasyMock.replay(producer, futureMock);
        // reconfiguring mock appender
        mockKafkaLog4jAppender.setKafkaProducer(producer);
        mockKafkaLog4jAppender.activateOptions();
    }

    private MockKafkaLog4jAppender getMockKafkaLog4jAppender() {
        return (MockKafkaLog4jAppender) Logger.getRootLogger().getAppender("KAFKA");
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
        // setting producer timeout (max.block.ms) to be low
        props.put("log4j.appender.KAFKA.maxBlockMs", "10");
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

