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
package org.apache.kafka.common.metrics;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class KafkaMetricsContextTest {

    private static final String SAMPLE_NAMESPACE = "sample-ns";

    private static final String LABEL_A_KEY = "label-a";
    private static final String LABEL_A_VALUE = "label-a-value";

    private String namespace;
    private Map<String, String> labels;
    private KafkaMetricsContext context;

    @Before
    public void beforeEach() {
        namespace = SAMPLE_NAMESPACE;
        labels = new HashMap<>();
        labels.put(LABEL_A_KEY, LABEL_A_VALUE);
    }

    @Test
    public void testCreationWithValidNamespaceAndNoLabels() {
        labels.clear();
        context = new KafkaMetricsContext(namespace, labels);

        assertEquals(1, context.contextLabels().size());
        assertEquals(namespace, context.contextLabels().get(MetricsContext.NAMESPACE));
    }

    @Test
    public void testCreationWithValidNamespaceAndLabels() {
        context = new KafkaMetricsContext(namespace, labels);

        assertEquals(2, context.contextLabels().size());
        assertEquals(namespace, context.contextLabels().get(MetricsContext.NAMESPACE));
        assertEquals(LABEL_A_VALUE, context.contextLabels().get(LABEL_A_KEY));
    }

    @Test
    public void testCreationWithValidNamespaceAndNullLabelValues() {
        labels.put(LABEL_A_KEY, null);
        context = new KafkaMetricsContext(namespace, labels);

        assertEquals(2, context.contextLabels().size());
        assertEquals(namespace, context.contextLabels().get(MetricsContext.NAMESPACE));
        assertNull(context.contextLabels().get(LABEL_A_KEY));
    }

    @Test
    public void testCreationWithNullNamespaceAndLabels() {
        context = new KafkaMetricsContext(null, labels);

        assertEquals(2, context.contextLabels().size());
        assertNull(context.contextLabels().get(MetricsContext.NAMESPACE));
        assertEquals(LABEL_A_VALUE, context.contextLabels().get(LABEL_A_KEY));
    }

    @Test
    public void testKafkaMetricsContextLabelsAreImmutable() {
        context = new KafkaMetricsContext(namespace, labels);
        assertThrows(UnsupportedOperationException.class, () -> context.contextLabels().clear());
    }
}