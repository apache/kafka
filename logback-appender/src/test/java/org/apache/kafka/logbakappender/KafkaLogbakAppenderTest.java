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
 */
package org.apache.kafka.logbakappender;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.status.Status;

public class KafkaLogbakAppenderTest {

    LoggerContext loggerContext = new LoggerContext();
    Logger logger = loggerContext.getLogger(KafkaLogbakAppenderTest.class);
    Logger root = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
    
    void configure(String file) throws JoranException {
        JoranConfigurator jc = new JoranConfigurator();
        jc.setContext(loggerContext);
        jc.doConfigure(KafkaLogbakAppenderTest.class.getResourceAsStream(file));
      }

    @Test
    public void testKafkaLogbackConfigs() throws JoranException {
        // topic missing
        configure("/logback-config-test.xml");
        List<Status> list = loggerContext.getStatusManager().getCopyOfStatusList();
        Status errorStatus = null;
        for(Status status : list){
            if(status.getLevel() == Status.ERROR){
                errorStatus = status;
                break;
            }
        }
        Assert.assertNotNull(errorStatus);
        Assert.assertEquals("Topic must be specified by the Kafka logbak appender", errorStatus.getMessage());
    }

    @Test
    public void testLogbackAppends() throws JoranException {
        configure("/logback-append-test.xml");

        for (int i = 1; i <= 5; ++i) {
            logger.error(getMessage(i));
        }

        Assert.assertEquals(
                5, ((MockKafkaLogbakAppender) (root.getAppender("KAFKA"))).getHistory().size());
    }

    private String getMessage(int i) {
        return "test_" + i;
    }
}

