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
package org.apache.kafka.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class PushHttpMetricsReporterTest {

    private static final URL URL;
    static {
        try {
            URL = new URL("http://fake:80");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private final Time time = new MockTime();

    private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    private final HttpURLConnection httpReq = mock(HttpURLConnection.class);
    private final OutputStream httpOut = mock(OutputStream.class);
    private final InputStream httpErr = mock(InputStream.class);
    private final ArgumentCaptor<Runnable> reportRunnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    private final ArgumentCaptor<byte[]> httpPayloadCaptor = ArgumentCaptor.forClass(byte[].class);

    private PushHttpMetricsReporter reporter;
    private MockedStatic<PushHttpMetricsReporter> mockedStaticReporter;

    @BeforeEach
    public void setUp() {
        reporter = new PushHttpMetricsReporter(time, executor);
        mockedStaticReporter = Mockito.mockStatic(PushHttpMetricsReporter.class);
    }

    @AfterEach
    public void tearDown() {
        mockedStaticReporter.close();
    }

    @Test
    public void testConfigureClose() throws Exception {
        whenClose();

        configure();
        reporter.close();

        verifyConfigure();
        verifyClose();
    }

    @Test
    public void testConfigureBadUrl() {
        Map<String, String> config = new HashMap<>();
        config.put(PushHttpMetricsReporter.METRICS_URL_CONFIG, "malformed;url");
        config.put(PushHttpMetricsReporter.METRICS_PERIOD_CONFIG, "5");
        assertThrows(ConfigException.class, () -> reporter.configure(config));
    }

    @Test
    public void testConfigureMissingPeriod() {
        Map<String, String> config = new HashMap<>();
        config.put(PushHttpMetricsReporter.METRICS_URL_CONFIG, URL.toString());
        assertThrows(ConfigException.class, () -> reporter.configure(config));
    }

    @Test
    public void testNoMetrics() throws Exception {
        whenRequest(200);

        configure();
        verifyConfigure();

        reportRunnableCaptor.getValue().run();
        verifyResponse();

        JsonNode payload = new ObjectMapper().readTree(httpPayloadCaptor.getValue());
        assertTrue(payload.isObject());

        assertPayloadHasClientInfo(payload);

        // Should contain an empty list of metrics, i.e. we report updates even if there are no metrics to report to
        // indicate liveness
        JsonNode metrics = payload.get("metrics");
        assertTrue(metrics.isArray());
        assertEquals(0, metrics.size());

        reporter.close();
        verifyClose();
    }

    // For error conditions, we expect them to come with a response body that we can read & log
    @Test
    public void testClientError() throws Exception {
        whenRequest(400, true);

        configure();
        verifyConfigure();

        reportRunnableCaptor.getValue().run();
        verifyResponse();

        reporter.close();
        verifyClose();
    }

    @Test
    public void testServerError() throws Exception {
        whenRequest(500, true);

        configure();
        verifyConfigure();

        reportRunnableCaptor.getValue().run();
        verifyResponse();

        reporter.close();
        verifyClose();
    }

    @Test
    public void testMetricValues() throws Exception {
        whenRequest(200);

        configure();
        verifyConfigure();
        KafkaMetric metric1 = new KafkaMetric(
                new Object(),
                new MetricName("name1", "group1", "desc1", Collections.singletonMap("key1", "value1")),
                new ImmutableValue<>(1.0),
                null,
                time
        );
        KafkaMetric newMetric1 = new KafkaMetric(
                new Object(),
                new MetricName("name1", "group1", "desc1", Collections.singletonMap("key1", "value1")),
                new ImmutableValue<>(-1.0),
                null,
                time
        );
        KafkaMetric metric2 = new KafkaMetric(
                new Object(),
                new MetricName("name2", "group2", "desc2", Collections.singletonMap("key2", "value2")),
                new ImmutableValue<>(2.0),
                null,
                time
        );
        KafkaMetric metric3 = new KafkaMetric(
                new Object(),
                new MetricName("name3", "group3", "desc3", Collections.singletonMap("key3", "value3")),
                new ImmutableValue<>(3.0),
                null,
                time
        );
        KafkaMetric metric4 = new KafkaMetric(
            new Object(),
            new MetricName("name4", "group4", "desc4", Collections.singletonMap("key4", "value4")),
            new ImmutableValue<>("value4"),
            null,
            time
        );

        reporter.init(Arrays.asList(metric1, metric2, metric4));
        reporter.metricChange(newMetric1); // added in init, modified
        reporter.metricChange(metric3); // added by change
        reporter.metricRemoval(metric2); // added in init, deleted by removal

        reportRunnableCaptor.getValue().run();
        verifyResponse();

        JsonNode payload = new ObjectMapper().readTree(httpPayloadCaptor.getValue());
        assertTrue(payload.isObject());
        assertPayloadHasClientInfo(payload);

        // We should be left with the modified version of metric1 and metric3
        JsonNode metrics = payload.get("metrics");
        assertTrue(metrics.isArray());
        assertEquals(3, metrics.size());
        List<JsonNode> metricsList = Arrays.asList(metrics.get(0), metrics.get(1), metrics.get(2));
        // Sort metrics based on name so that we can verify the value for each metric below
        metricsList.sort(Comparator.comparing(m -> m.get("name").textValue()));

        JsonNode m1 = metricsList.get(0);
        assertEquals("name1", m1.get("name").textValue());
        assertEquals("group1", m1.get("group").textValue());
        JsonNode m1Tags = m1.get("tags");
        assertTrue(m1Tags.isObject());
        assertEquals(1, m1Tags.size());
        assertEquals("value1", m1Tags.get("key1").textValue());
        assertEquals(-1.0, m1.get("value").doubleValue(), 0.0);

        JsonNode m3 = metricsList.get(1);
        assertEquals("name3", m3.get("name").textValue());
        assertEquals("group3", m3.get("group").textValue());
        JsonNode m3Tags = m3.get("tags");
        assertTrue(m3Tags.isObject());
        assertEquals(1, m3Tags.size());
        assertEquals("value3", m3Tags.get("key3").textValue());
        assertEquals(3.0, m3.get("value").doubleValue(), 0.0);

        JsonNode m4 = metricsList.get(2);
        assertEquals("name4", m4.get("name").textValue());
        assertEquals("group4", m4.get("group").textValue());
        JsonNode m4Tags = m4.get("tags");
        assertTrue(m4Tags.isObject());
        assertEquals(1, m4Tags.size());
        assertEquals("value4", m4Tags.get("key4").textValue());
        assertEquals("value4", m4.get("value").textValue());

        reporter.close();
        verifyClose();
    }

    private void configure() {
        Map<String, String> config = new HashMap<>();
        config.put(PushHttpMetricsReporter.METRICS_URL_CONFIG, URL.toString());
        config.put(PushHttpMetricsReporter.METRICS_PERIOD_CONFIG, "5");
        reporter.configure(config);
    }

    private void whenRequest(int returnStatus) throws Exception {
        whenRequest(returnStatus, false);
    }

    // Expect that a request is made with the given response code
    private void whenRequest(int returnStatus, boolean readResponse) throws Exception {
        when(PushHttpMetricsReporter.newHttpConnection(URL)).thenReturn(httpReq);
        when(httpReq.getOutputStream()).thenReturn(httpOut);
        when(httpReq.getResponseCode()).thenReturn(returnStatus);
        if (readResponse)
            whenReadResponse();
    }

    private void assertPayloadHasClientInfo(JsonNode payload) throws UnknownHostException {
        // Should contain client info...
        JsonNode client = payload.get("client");
        assertTrue(client.isObject());
        assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), client.get("host").textValue());
        assertEquals("", client.get("client_id").textValue());
        assertEquals(time.milliseconds(), client.get("time").longValue());
    }

    private void whenReadResponse() {
        when(httpReq.getErrorStream()).thenReturn(httpErr);
        when(PushHttpMetricsReporter.readResponse(httpErr)).thenReturn("error response message");
    }

    private void whenClose() throws Exception {
        when(executor.awaitTermination(anyLong(), any())).thenReturn(true);
    }

    private void verifyClose() throws InterruptedException {
        InOrder inOrder = inOrder(executor);
        inOrder.verify(executor).shutdown();
        inOrder.verify(executor).awaitTermination(30L, TimeUnit.SECONDS);
    }

    private void verifyConfigure() {
        verify(executor).scheduleAtFixedRate(reportRunnableCaptor.capture(),
            eq(5L), eq(5L), eq(TimeUnit.SECONDS));
    }

    private void verifyResponse() throws IOException {
        verify(httpReq).setRequestMethod("POST");
        verify(httpReq).setDoInput(true);
        verify(httpReq).setRequestProperty("Content-Type", "application/json");
        verify(httpReq).setRequestProperty(eq("Content-Length"), anyString());
        verify(httpReq).setRequestProperty("Accept", "*/*");
        verify(httpReq).setUseCaches(false);
        verify(httpReq).setDoOutput(true);
        verify(httpReq).disconnect();

        verify(httpOut).write(httpPayloadCaptor.capture());
        verify(httpOut).flush();
        verify(httpOut).close();
    }

    static class ImmutableValue<T> implements Gauge<T> {
        private final T value;

        public ImmutableValue(T value) {
            this.value = value;
        }

        @Override
        public T value(MetricConfig config, long now) {
            return value;
        }
    }
}
