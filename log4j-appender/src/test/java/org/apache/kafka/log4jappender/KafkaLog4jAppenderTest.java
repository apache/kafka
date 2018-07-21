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

import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaLog4jAppenderTest {

    private Logger logger = Logger.getLogger(KafkaLog4jAppenderTest.class);

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
                5, ((MockKafkaLog4jAppender) (Logger.getRootLogger().getAppender("KAFKA"))).getHistory().size());
    }

    @Test(expected = RuntimeException.class)
    public void testLog4jAppendsWithSyncSendAndSimulateProducerFailShouldThrowException() {
        Properties props = getLog4jConfig(true);
        props.put("log4j.appender.KAFKA.IgnoreExceptions", "false");
        // property for mock appender
        props.put("log4j.appender.KAFKA.FailSync", "true");
        PropertyConfigurator.configure(props);

        logger.error(getMessage(0));

        Assert.fail();
    }

    @Test
    public void testLog4jAppendsWithSyncSendShouldNotThrowException() {
        Properties props = getLog4jConfig(true);
        props.put("log4j.appender.KAFKA.IgnoreExceptions", "false");
        PropertyConfigurator.configure(props);

        logger.error(getMessage(0));

        Assert.assertEquals(
                1, ((MockKafkaLog4jAppender) (Logger.getRootLogger().getAppender("KAFKA"))).getHistory().size());
    }

    private byte[] getMessage(int i) {
        return ("test_" + i).getBytes(StandardCharsets.UTF_8);
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
        props.put("log4j.appender.KAFKA.SyncSend", syncSend ? "true" : "false");
        props.put("log4j.logger.kafka.log4j", "INFO, KAFKA");
        return props;
    }
}

