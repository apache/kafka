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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.common.utils.MockTime;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProcessingContext.class})
public class RetryWithToleranceExecutorTest {

    @Mock
    private ProcessingContext processingContext;

    Object ref = new Object();

    @Test
    public void testHandleExceptionInTransformations() {
        testHandleExceptionInStage(Stage.TRANSFORMATION, new Exception());
    }

    @Test
    public void testHandleExceptionInHeaderConverter() {
        testHandleExceptionInStage(Stage.HEADER_CONVERTER, new Exception());
    }

    @Test
    public void testHandleExceptionInValueConverter() {
        testHandleExceptionInStage(Stage.VALUE_CONVERTER, new Exception());
    }

    @Test
    public void testHandleExceptionInKeyConverter() {
        testHandleExceptionInStage(Stage.KEY_CONVERTER, new Exception());
    }

    @Test
    public void testHandleExceptionInTaskPut() {
        testHandleExceptionInStage(Stage.TASK_PUT, new org.apache.kafka.connect.errors.RetriableException("Test"));
    }

    @Test
    public void testHandleExceptionInTaskPoll() {
        testHandleExceptionInStage(Stage.TASK_POLL, new org.apache.kafka.connect.errors.RetriableException("Test"));
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInTaskPut() {
        testHandleExceptionInStage(Stage.TASK_PUT, new Exception());
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInTaskPoll() {
        testHandleExceptionInStage(Stage.TASK_POLL, new Exception());
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInKafkaConsume() {
        testHandleExceptionInStage(Stage.KAFKA_CONSUME, new Exception());
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInKafkaProduce() {
        testHandleExceptionInStage(Stage.KAFKA_PRODUCE, new Exception());
    }

    private void testHandleExceptionInStage(Stage type, Exception ex) {
        RetryWithToleranceExecutor executor = setupExecutor();
        setupProcessingContext(type, ex);
        replay(processingContext);
        assertNotNull(executor.execute(new ExceptionThrower(ex), processingContext).error());
        PowerMock.verifyAll();
    }

    private RetryWithToleranceExecutor setupExecutor() {
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor();
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRY_TIMEOUT, "0");
        props.put(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "all");
        executor.configure(props);
        return executor;
    }

    private void setupProcessingContext(Stage type, Exception ex) {
        EasyMock.expect(processingContext.stage()).andReturn(type).anyTimes();
        EasyMock.expect(processingContext.result()).andReturn(new Result(ex));
        EasyMock.expect(processingContext.result(anyObject(Result.class))).andReturn(processingContext);

        EasyMock.expectLastCall();

        processingContext.report();
        EasyMock.expectLastCall();
    }

    @Test
    public void testCheckRetryLimit() {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRY_TIMEOUT, "500");
        props.put(RetryWithToleranceExecutor.RETRY_DELAY_MAX_MS, "100");
        executor.configure(props);

        time.setCurrentTimeMs(100);
        assertTrue(executor.checkRetry(0));

        time.setCurrentTimeMs(200);
        assertTrue(executor.checkRetry(0));

        time.setCurrentTimeMs(400);
        assertTrue(executor.checkRetry(0));

        time.setCurrentTimeMs(499);
        assertTrue(executor.checkRetry(0));

        time.setCurrentTimeMs(501);
        assertFalse(executor.checkRetry(0));

        time.setCurrentTimeMs(600);
        assertFalse(executor.checkRetry(0));
    }

    @Test
    public void testBackoffLimit() {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);

        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRY_TIMEOUT, "5");
        props.put(RetryWithToleranceExecutor.RETRY_DELAY_MAX_MS, "5000");
        executor.configure(props);

        replay(processingContext);

        long prevTs = time.hiResClockMs();
        executor.backoff(1);
        assertEquals(time.hiResClockMs() - prevTs, 300);

        prevTs = time.hiResClockMs();
        executor.backoff(2);
        assertEquals(time.hiResClockMs() - prevTs, 600);

        prevTs = time.hiResClockMs();
        executor.backoff(3);
        assertEquals(time.hiResClockMs() - prevTs, 1200);

        prevTs = time.hiResClockMs();
        executor.backoff(4);
        assertEquals(time.hiResClockMs() - prevTs, 2400);

        prevTs = time.hiResClockMs();
        executor.backoff(5);
        assertEquals(time.hiResClockMs() - prevTs, 4800);

        prevTs = time.hiResClockMs();
        executor.backoff(6);
        assertTrue(time.hiResClockMs() - prevTs < 5000);


        PowerMock.verifyAll();
    }


    @Test
    public void testDefaultConfigs() {
        RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig configuration;
        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(new HashMap<>());
        assertEquals(configuration.retryTimeout(), 0);
        assertEquals(configuration.retryDelayMax(), 60000);
        assertEquals(configuration.toleranceLimit(), ToleranceType.NONE);

        PowerMock.verifyAll();
    }

    @Test
    public void testConfigs() {
        RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig configuration;
        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("retry.timeout", "100"));
        assertEquals(configuration.retryTimeout(), 100);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("retry.delay.max.ms", "100"));
        assertEquals(configuration.retryDelayMax(), 100);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("allowed.max", "none"));
        assertEquals(configuration.toleranceLimit(), ToleranceType.NONE);

        PowerMock.verifyAll();
    }

    Map<String, Object> config(String key, Object val) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(key, val);
        return configs;
    }

    private static class ExceptionThrower implements Operation {
        private Exception e;

        public ExceptionThrower(Exception e) {
            this.e = e;
        }

        @Override
        public Object apply() throws Exception {
            throw e;
        }
    }
}