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
package org.apache.kafka.storage.log.metrics;

import com.yammer.metrics.core.Meter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BrokerTopicPartitionMetricsTest {

    @Test
    public void testMetersLazyInitAndClose() {
        BrokerTopicPartitionMetrics m = new BrokerTopicPartitionMetrics("t", 1);

        Meter bytesIn = m.bytesInRate();
        Meter bytesOut = m.bytesOutRate();
        Meter messagesIn = m.messagesInRate();
        Meter bytesRejected = m.bytesRejectedRate();
        Meter totalProduce = m.totalProduceRequestRate();
        Meter totalFetch = m.totalFetchRequestRate();
        Meter failedProduce = m.failedProduceRequestRate();
        Meter failedFetch = m.failedFetchRequestRate();
        Meter fetchConversions = m.fetchMessageConversionsRate();
        Meter produceConversions = m.produceMessageConversionsRate();

        assertNotNull(bytesIn);
        assertNotNull(bytesOut);
        assertNotNull(messagesIn);
        assertNotNull(bytesRejected);
        assertNotNull(totalProduce);
        assertNotNull(totalFetch);
        assertNotNull(failedProduce);
        assertNotNull(failedFetch);
        assertNotNull(fetchConversions);
        assertNotNull(produceConversions);

        m.close();

        // After close, calling again should recreate lazily without throwing
        assertNotNull(m.bytesInRate());
        assertNotNull(m.messagesInRate());
        m.close();
    }
}


