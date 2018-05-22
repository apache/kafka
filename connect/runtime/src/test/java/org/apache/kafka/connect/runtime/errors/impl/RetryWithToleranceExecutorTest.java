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
package org.apache.kafka.connect.runtime.errors.impl;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.runtime.errors.OperationExecutor;
import org.apache.kafka.connect.runtime.errors.ProcessingContext;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.StageType;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProcessingContext.class})
public class RetryWithToleranceExecutorTest {

    @Mock
    private ProcessingContext processingContext;

    Object ref = new Object();

    @Test
    public void testHandleExceptionInTransformations() {
        testHandleExceptionInStage(StageType.TRANSFORMATION, new Exception());
    }

    @Test
    public void testHandleExceptionInHeaderConverter() {
        testHandleExceptionInStage(StageType.HEADER_CONVERTER, new Exception());
    }

    @Test
    public void testHandleExceptionInValueConverter() {
        testHandleExceptionInStage(StageType.VALUE_CONVERTER, new Exception());
    }

    @Test
    public void testHandleExceptionInKeyConverter() {
        testHandleExceptionInStage(StageType.KEY_CONVERTER, new Exception());
    }

    @Test
    public void testHandleExceptionInKafkaConsume() {
        testHandleExceptionInStage(StageType.KAFKA_CONSUME, new org.apache.kafka.common.errors.RetriableException() {});
    }

    @Test
    public void testHandleExceptionInKafkaProduce() {
        testHandleExceptionInStage(StageType.KAFKA_PRODUCE, new org.apache.kafka.common.errors.RetriableException() {});
    }

    @Test
    public void testHandleExceptionInTaskPut() {
        testHandleExceptionInStage(StageType.TASK_PUT, new org.apache.kafka.connect.errors.RetriableException("Test"));
    }

    @Test
    public void testHandleExceptionInTaskPoll() {
        testHandleExceptionInStage(StageType.TASK_POLL, new org.apache.kafka.connect.errors.RetriableException("Test"));
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInTaskPut() {
        testHandleExceptionInStage(StageType.TASK_PUT, new Exception());
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInTaskPoll() {
        testHandleExceptionInStage(StageType.TASK_POLL, new Exception());
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInKafkaConsume() {
        testHandleExceptionInStage(StageType.KAFKA_CONSUME, new Exception());
    }

    @Test(expected = Exception.class)
    public void testThrowExceptionInKafkaProduce() {
        testHandleExceptionInStage(StageType.KAFKA_PRODUCE, new Exception());
    }

    private void testHandleExceptionInStage(StageType type, Exception ex) {
        RetryWithToleranceExecutor executor = setupExecutor();
        setupProcessingContext(type, ex);
        replay(processingContext);
        assertEquals(ref, executor.execute(new ExceptionThrower(ex), ref, processingContext));
        PowerMock.verifyAll();
    }

    private RetryWithToleranceExecutor setupExecutor() {
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor();
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRIES_LIMIT, "0");
        props.put(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "10");
        props.put(RetryWithToleranceExecutor.TOLERANCE_RATE_LIMIT, "10");
        executor.configure(props);
        return executor;
    }

    private void setupProcessingContext(StageType type, Exception ex) {
        EasyMock.expect(processingContext.current()).andReturn(Stage.newBuilder(type).build());
        EasyMock.expect(processingContext.exception()).andReturn(ex);
        EasyMock.expect(processingContext.attempt()).andReturn(1);
        EasyMock.expect(processingContext.timeOfError()).andReturn(System.currentTimeMillis());
        processingContext.setTimeOfError(EasyMock.anyLong());

        processingContext.incrementAttempt();
        EasyMock.expectLastCall();

        processingContext.report();
        EasyMock.expectLastCall();

        processingContext.setException(ex);
        EasyMock.expectLastCall();
    }

    @Test
    public void testCheckRetryLimit() {
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor();
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRIES_LIMIT, "5");
        props.put(RetryWithToleranceExecutor.RETRIES_DELAY_MAX_MS, "120000");
        executor.configure(props);

        EasyMock.expect(processingContext.attempt()).andReturn(1);
        EasyMock.expect(processingContext.attempt()).andReturn(2);
        EasyMock.expect(processingContext.attempt()).andReturn(3);
        EasyMock.expect(processingContext.attempt()).andReturn(4);
        EasyMock.expect(processingContext.attempt()).andReturn(5);
        EasyMock.expect(processingContext.attempt()).andReturn(6);

        replay(processingContext);

        assertTrue(executor.checkRetry(processingContext));
        assertTrue(executor.checkRetry(processingContext));
        assertTrue(executor.checkRetry(processingContext));
        assertTrue(executor.checkRetry(processingContext));
        assertTrue(executor.checkRetry(processingContext));
        assertFalse(executor.checkRetry(processingContext));
    }

    @Test
    public void testBackoffLimit() {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);

        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRIES_LIMIT, "5");
        props.put(RetryWithToleranceExecutor.RETRIES_DELAY_MAX_MS, "10000");
        executor.configure(props);

        EasyMock.expect(processingContext.attempt()).andReturn(1);
        EasyMock.expect(processingContext.attempt()).andReturn(2);
        EasyMock.expect(processingContext.attempt()).andReturn(3);
        EasyMock.expect(processingContext.attempt()).andReturn(4);
        EasyMock.expect(processingContext.attempt()).andReturn(5);

        replay(processingContext);

        long prevTs = time.hiResClockMs();
        executor.backoff(processingContext);
        assertEquals(time.hiResClockMs() - prevTs, 1000);

        prevTs = time.hiResClockMs();
        executor.backoff(processingContext);
        assertEquals(time.hiResClockMs() - prevTs, 2000);

        prevTs = time.hiResClockMs();
        executor.backoff(processingContext);
        assertEquals(time.hiResClockMs() - prevTs, 4000);

        prevTs = time.hiResClockMs();
        executor.backoff(processingContext);
        assertEquals(time.hiResClockMs() - prevTs, 8000);

        prevTs = time.hiResClockMs();
        executor.backoff(processingContext);
        assertTrue(time.hiResClockMs() - prevTs < 10000);

        PowerMock.verifyAll();
    }

    @Test
    public void testToleranceLimitWithinDuration() {
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(new MockTime(0, 0, 0));
        Map<String, Object> props = config(RetryWithToleranceExecutor.TOLERANCE_RATE_LIMIT, "1");
        props.put(RetryWithToleranceExecutor.TOLERANCE_RATE_DURATION, "minute");
        executor.configure(props);

        long errorTimeMs = 1;
        EasyMock.expect(processingContext.timeOfError()).andReturn(errorTimeMs);
        EasyMock.expect(processingContext.timeOfError()).andReturn(errorTimeMs + 1);

        replay(processingContext);

        assertTrue(executor.withinToleranceLimits(processingContext));
        assertFalse(executor.withinToleranceLimits(processingContext));

        PowerMock.verifyAll();
    }

    @Test
    public void testToleranceLimitAcrossDuration() {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);
        Map<String, Object> props = config(RetryWithToleranceExecutor.TOLERANCE_RATE_LIMIT, "1");
        props.put(RetryWithToleranceExecutor.TOLERANCE_RATE_DURATION, "minute");
        executor.configure(props);

        // first error occurs at 59 seconds
        long errorTimeMs = 59000;
        EasyMock.expect(processingContext.timeOfError()).andReturn(errorTimeMs);
        // second error occurs at 61 seconds
        errorTimeMs = 61000;
        EasyMock.expect(processingContext.timeOfError()).andReturn(errorTimeMs);

        replay(processingContext);

        assertTrue(executor.withinToleranceLimits(processingContext));

        time.setCurrentTimeMs(60000);

        assertTrue(executor.withinToleranceLimits(processingContext));

        PowerMock.verifyAll();
    }

    @Test
    public void testDefaultConfigs() {
        RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig configuration;
        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(new HashMap<>());
        assertEquals(configuration.retriesLimit(), 0);
        assertEquals(configuration.retriesDelayMax(), 60000);
        assertEquals(configuration.toleranceLimit(), 0);
        assertEquals(configuration.toleranceRateLimit(), 0);
        assertEquals(configuration.toleranceRateDuration(), TimeUnit.MINUTES);

        PowerMock.verifyAll();
    }

    @Test
    public void testConfigs() {
        RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig configuration;
        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("retries.limit", "100"));
        assertEquals(configuration.retriesLimit(), 100);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("retries.delay.max.ms", "100"));
        assertEquals(configuration.retriesDelayMax(), 100);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("tolerance.limit", "1024"));
        assertEquals(configuration.toleranceLimit(), 1024);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("tolerance.rate.limit", "125"));
        assertEquals(configuration.toleranceRateLimit(), 125);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("tolerance.rate.duration", "minute"));
        assertEquals(configuration.toleranceRateDuration(), TimeUnit.MINUTES);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("tolerance.rate.duration", "day"));
        assertEquals(configuration.toleranceRateDuration(), TimeUnit.DAYS);

        configuration = new RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig(config("tolerance.rate.duration", "hour"));
        assertEquals(configuration.toleranceRateDuration(), TimeUnit.HOURS);

        PowerMock.verifyAll();
    }

    Map<String, Object> config(String key, Object val) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(key, val);
        return configs;
    }

    private static class ExceptionThrower implements OperationExecutor.Operation {
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
