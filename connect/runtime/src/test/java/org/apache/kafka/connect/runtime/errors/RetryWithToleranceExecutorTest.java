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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.errors.RetryWithToleranceExecutor.RetryWithToleranceExecutorConfig;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProcessingContext.class})
@PowerMockIgnore("javax.management.*")
public class RetryWithToleranceExecutorTest {

    private ProcessingContext processingContext;

    @SuppressWarnings("unused")
    @Mock
    private Operation<String> mockOperation;

    @Mock
    ErrorHandlingMetrics errorHandlingMetrics;

    @Before
    public void setup() {
        processingContext = new ProcessingContext();
    }

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

    @Test(expected = ConnectException.class)
    public void testThrowExceptionInTaskPut() {
        testHandleExceptionInStage(Stage.TASK_PUT, new Exception());
    }

    @Test(expected = ConnectException.class)
    public void testThrowExceptionInTaskPoll() {
        testHandleExceptionInStage(Stage.TASK_POLL, new Exception());
    }

    @Test(expected = ConnectException.class)
    public void testThrowExceptionInKafkaConsume() {
        testHandleExceptionInStage(Stage.KAFKA_CONSUME, new Exception());
    }

    @Test(expected = ConnectException.class)
    public void testThrowExceptionInKafkaProduce() {
        testHandleExceptionInStage(Stage.KAFKA_PRODUCE, new Exception());
    }

    private void testHandleExceptionInStage(Stage type, Exception ex) {
        RetryWithToleranceExecutor executor = setupExecutor();
        setupProcessingContext(type);

        executor.execute(new ExceptionThrower(ex), processingContext);
        assertNotNull(processingContext.error());
        PowerMock.verifyAll();
    }

    private RetryWithToleranceExecutor setupExecutor() {
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor();
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRY_TIMEOUT, "0");
        props.put(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "all");
        executor.configure(props);
        executor.setMetrics(errorHandlingMetrics);
        return executor;
    }

    private void setupProcessingContext(Stage type) {
        processingContext.position(type);
    }

    @Test
    public void testExecAndHandleRetriableErrorOnce() throws Exception {
        execAndHandleRetriableError(1, 300, new RetriableException("Test"));
    }

    @Test
    public void testExecAndHandleRetriableErrorThrice() throws Exception {
        execAndHandleRetriableError(3, 2100, new RetriableException("Test"));
    }

    @Test
    public void testExecAndHandleNonRetriableErrorOnce() throws Exception {
        execAndHandleNonRetriableError(1, 0, new Exception("Non Retriable Test"));
    }

    @Test
    public void testExecAndHandleNonRetriableErrorThrice() throws Exception {
        execAndHandleNonRetriableError(3, 0, new Exception("Non Retriable Test"));
    }

    public void execAndHandleRetriableError(int numRetriableExceptionsThrown, long expectedWait, Exception e) throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRY_TIMEOUT, "6000");
        props.put(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "all");
        executor.configure(props);
        executor.setMetrics(errorHandlingMetrics);

        EasyMock.expect(mockOperation.apply()).andThrow(e).times(numRetriableExceptionsThrown);
        EasyMock.expect(mockOperation.apply()).andReturn("Success");

        ProcessingContext context = new ProcessingContext();
        context.setCurrentContext(Stage.TRANSFORMATION, ExceptionThrower.class);

        replay(mockOperation);

        String result = executor.execAndHandleError(mockOperation, context, Exception.class);
        assertFalse(context.failed());
        assertEquals("Success", result);
        assertEquals(expectedWait, time.hiResClockMs());

        PowerMock.verifyAll();
    }

    public void execAndHandleNonRetriableError(int numRetriableExceptionsThrown, long expectedWait, Exception e) throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor(time);
        Map<String, Object> props = config(RetryWithToleranceExecutor.RETRY_TIMEOUT, "6000");
        props.put(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "all");
        executor.configure(props);
        executor.setMetrics(errorHandlingMetrics);

        EasyMock.expect(mockOperation.apply()).andThrow(e).times(numRetriableExceptionsThrown);
        EasyMock.expect(mockOperation.apply()).andReturn("Success");

        ProcessingContext context = new ProcessingContext();
        context.setCurrentContext(Stage.TRANSFORMATION, ExceptionThrower.class);

        replay(mockOperation);

        String result = executor.execAndHandleError(mockOperation, context, Exception.class);
        assertTrue(context.failed());
        assertNull(result);
        assertEquals(expectedWait, time.hiResClockMs());
        assertEquals(1, context.attempt());

        PowerMock.verifyAll();
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

        long prevTs = time.hiResClockMs();
        executor.backoff(1, 5000);
        assertEquals(300, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        executor.backoff(2, 5000);
        assertEquals(600, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        executor.backoff(3, 5000);
        assertEquals(1200, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        executor.backoff(4, 5000);
        assertEquals(2400, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        executor.backoff(5, 5000);
        assertEquals(500, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        executor.backoff(6, 5000);
        assertEquals(0, time.hiResClockMs() - prevTs);

        PowerMock.verifyAll();
    }

    @Test
    public void testToleranceLimit() {
        RetryWithToleranceExecutor executor = new RetryWithToleranceExecutor();
        executor.configure(config(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "none"));
        executor.setMetrics(errorHandlingMetrics);
        executor.markAsFailed();
        assertFalse("should not tolerate any errors", executor.withinToleranceLimits());

        executor = new RetryWithToleranceExecutor();
        executor.configure(config(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "all"));
        executor.setMetrics(errorHandlingMetrics);
        executor.markAsFailed();
        executor.markAsFailed();
        assertTrue("should tolerate all errors", executor.withinToleranceLimits());

        executor = new RetryWithToleranceExecutor();
        executor.configure(config(RetryWithToleranceExecutor.TOLERANCE_LIMIT, "none"));
        assertTrue("no tolerance is within limits if no failures", executor.withinToleranceLimits());
    }

    @Test
    public void testDefaultConfigs() {
        RetryWithToleranceExecutorConfig configuration;
        configuration = new RetryWithToleranceExecutorConfig(new HashMap<>());
        assertEquals(configuration.retryTimeout(), 0);
        assertEquals(configuration.retryDelayMax(), 60000);
        assertEquals(configuration.toleranceLimit(), ToleranceType.NONE);

        PowerMock.verifyAll();
    }

    @Test
    public void testConfigs() {
        RetryWithToleranceExecutorConfig configuration;
        configuration = new RetryWithToleranceExecutorConfig(config("retry.timeout", "100"));
        assertEquals(configuration.retryTimeout(), 100);

        configuration = new RetryWithToleranceExecutorConfig(config("retry.delay.max.ms", "100"));
        assertEquals(configuration.retryDelayMax(), 100);

        configuration = new RetryWithToleranceExecutorConfig(config("allowed.max", "none"));
        assertEquals(configuration.toleranceLimit(), ToleranceType.NONE);

        PowerMock.verifyAll();
    }

    Map<String, Object> config(String key, Object val) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(key, val);
        return configs;
    }

    private static class ExceptionThrower implements Operation<Object> {
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
