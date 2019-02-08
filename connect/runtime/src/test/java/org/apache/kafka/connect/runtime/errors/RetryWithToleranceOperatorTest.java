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
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkTask;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_DEFAULT;
import static org.apache.kafka.connect.runtime.errors.ToleranceType.ALL;
import static org.apache.kafka.connect.runtime.errors.ToleranceType.NONE;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ProcessingContext.class})
@PowerMockIgnore("javax.management.*")
public class RetryWithToleranceOperatorTest {

    public static final RetryWithToleranceOperator NOOP_OPERATOR = new RetryWithToleranceOperator(
            ERRORS_RETRY_TIMEOUT_DEFAULT, ERRORS_RETRY_MAX_DELAY_DEFAULT, NONE, SYSTEM);
    static {
        NOOP_OPERATOR.metrics(new ErrorHandlingMetrics());
    }

    @SuppressWarnings("unused")
    @Mock
    private Operation<String> mockOperation;

    @Mock
    ErrorHandlingMetrics errorHandlingMetrics;

    @Mock
    Plugins plugins;

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
        RetryWithToleranceOperator retryWithToleranceOperator = setupExecutor();
        retryWithToleranceOperator.execute(new ExceptionThrower(ex), type, ExceptionThrower.class);
        assertTrue(retryWithToleranceOperator.failed());
        PowerMock.verifyAll();
    }

    private RetryWithToleranceOperator setupExecutor() {
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(0, ERRORS_RETRY_MAX_DELAY_DEFAULT, ALL, SYSTEM);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);
        return retryWithToleranceOperator;
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
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(6000, ERRORS_RETRY_MAX_DELAY_DEFAULT, ALL, time);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);

        EasyMock.expect(mockOperation.call()).andThrow(e).times(numRetriableExceptionsThrown);
        EasyMock.expect(mockOperation.call()).andReturn("Success");

        replay(mockOperation);

        String result = retryWithToleranceOperator.execAndHandleError(mockOperation, Exception.class);
        assertFalse(retryWithToleranceOperator.failed());
        assertEquals("Success", result);
        assertEquals(expectedWait, time.hiResClockMs());

        PowerMock.verifyAll();
    }

    public void execAndHandleNonRetriableError(int numRetriableExceptionsThrown, long expectedWait, Exception e) throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(6000, ERRORS_RETRY_MAX_DELAY_DEFAULT, ALL, time);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);

        EasyMock.expect(mockOperation.call()).andThrow(e).times(numRetriableExceptionsThrown);
        EasyMock.expect(mockOperation.call()).andReturn("Success");

        replay(mockOperation);

        String result = retryWithToleranceOperator.execAndHandleError(mockOperation, Exception.class);
        assertTrue(retryWithToleranceOperator.failed());
        assertNull(result);
        assertEquals(expectedWait, time.hiResClockMs());

        PowerMock.verifyAll();
    }

    @Test
    public void testCheckRetryLimit() {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(500, 100, NONE, time);

        time.setCurrentTimeMs(100);
        assertTrue(retryWithToleranceOperator.checkRetry(0));

        time.setCurrentTimeMs(200);
        assertTrue(retryWithToleranceOperator.checkRetry(0));

        time.setCurrentTimeMs(400);
        assertTrue(retryWithToleranceOperator.checkRetry(0));

        time.setCurrentTimeMs(499);
        assertTrue(retryWithToleranceOperator.checkRetry(0));

        time.setCurrentTimeMs(501);
        assertFalse(retryWithToleranceOperator.checkRetry(0));

        time.setCurrentTimeMs(600);
        assertFalse(retryWithToleranceOperator.checkRetry(0));
    }

    @Test
    public void testBackoffLimit() {
        MockTime time = new MockTime(0, 0, 0);
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(5, 5000, NONE, time);

        long prevTs = time.hiResClockMs();
        retryWithToleranceOperator.backoff(1, 5000);
        assertEquals(300, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        retryWithToleranceOperator.backoff(2, 5000);
        assertEquals(600, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        retryWithToleranceOperator.backoff(3, 5000);
        assertEquals(1200, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        retryWithToleranceOperator.backoff(4, 5000);
        assertEquals(2400, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        retryWithToleranceOperator.backoff(5, 5000);
        assertEquals(500, time.hiResClockMs() - prevTs);

        prevTs = time.hiResClockMs();
        retryWithToleranceOperator.backoff(6, 5000);
        assertEquals(0, time.hiResClockMs() - prevTs);

        PowerMock.verifyAll();
    }

    @Test
    public void testToleranceLimit() {
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(ERRORS_RETRY_TIMEOUT_DEFAULT, ERRORS_RETRY_MAX_DELAY_DEFAULT, NONE, SYSTEM);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);
        retryWithToleranceOperator.markAsFailed();
        assertFalse("should not tolerate any errors", retryWithToleranceOperator.withinToleranceLimits());

        retryWithToleranceOperator = new RetryWithToleranceOperator(ERRORS_RETRY_TIMEOUT_DEFAULT, ERRORS_RETRY_MAX_DELAY_DEFAULT, ALL, SYSTEM);
        retryWithToleranceOperator.metrics(errorHandlingMetrics);
        retryWithToleranceOperator.markAsFailed();
        retryWithToleranceOperator.markAsFailed();
        assertTrue("should tolerate all errors", retryWithToleranceOperator.withinToleranceLimits());

        retryWithToleranceOperator = new RetryWithToleranceOperator(ERRORS_RETRY_TIMEOUT_DEFAULT, ERRORS_RETRY_MAX_DELAY_DEFAULT, NONE, SYSTEM);
        assertTrue("no tolerance is within limits if no failures", retryWithToleranceOperator.withinToleranceLimits());
    }

    @Test
    public void testDefaultConfigs() {
        ConnectorConfig configuration = config(emptyMap());
        assertEquals(configuration.errorRetryTimeout(), ERRORS_RETRY_TIMEOUT_DEFAULT);
        assertEquals(configuration.errorMaxDelayInMillis(), ERRORS_RETRY_MAX_DELAY_DEFAULT);
        assertEquals(configuration.errorToleranceType(), ERRORS_TOLERANCE_DEFAULT);

        PowerMock.verifyAll();
    }

    ConnectorConfig config(Map<String, String> connProps) {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfig.NAME_CONFIG, "test");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, SinkTask.class.getName());
        props.putAll(connProps);
        return new ConnectorConfig(plugins, props);
    }

    @Test
    public void testSetConfigs() {
        ConnectorConfig configuration;
        configuration = config(singletonMap(ERRORS_RETRY_TIMEOUT_CONFIG, "100"));
        assertEquals(configuration.errorRetryTimeout(), 100);

        configuration = config(singletonMap(ERRORS_RETRY_MAX_DELAY_CONFIG, "100"));
        assertEquals(configuration.errorMaxDelayInMillis(), 100);

        configuration = config(singletonMap(ERRORS_TOLERANCE_CONFIG, "none"));
        assertEquals(configuration.errorToleranceType(), ToleranceType.NONE);

        PowerMock.verifyAll();
    }

    private static class ExceptionThrower implements Operation<Object> {
        private Exception e;

        public ExceptionThrower(Exception e) {
            this.e = e;
        }

        @Override
        public Object call() throws Exception {
            throw e;
        }
    }
}
