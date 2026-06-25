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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.network.Request;
import org.apache.kafka.network.Session;
import org.apache.kafka.network.metrics.RequestChannelMetrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

public class BaseClientQuotaManagerTest {

    protected final MockTime time = new MockTime();
    protected int numCallbacks = 0;
    protected Metrics metrics;

    @BeforeEach
    public void setup() {
        metrics = new Metrics(new MetricConfig(), List.of(), time);
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    protected final ThrottleCallback callback = new ThrottleCallback() {
        @Override
        public void startThrottling() {}

        @Override
        public void endThrottling() {
            // Count how many times this callback is called for notifyThrottlingDone().
            numCallbacks += 1;
        }
    };

    protected Session buildSession(String user) {
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user);
        return new Session(principal, null);
    }

    protected int maybeRecord(ClientQuotaManager quotaManager, String user, String clientId, double value) {
        return quotaManager.maybeRecordAndGetThrottleTimeMs(buildSession(user), clientId, value, time.milliseconds());
    }

    protected void throttle(ClientQuotaManager quotaManager, int throttleTimeMs, ThrottleCallback channelThrottlingCallback) {
        AbstractRequest.Builder<FetchRequest> builder = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), 0, 1000, Map.of());
        FetchRequest fetchRequest = builder.build();
        ByteBuffer buffer = fetchRequest.serializeWithHeader(new RequestHeader(builder.apiKey(), fetchRequest.version(), "", 0));
        RequestChannelMetrics requestChannelMetrics = mock(RequestChannelMetrics.class);

        // read the header from the buffer first so that the body can be read next from the Request constructor
        RequestHeader header = RequestHeader.parse(buffer);
        RequestContext context = new RequestContext(header, "1", assertDoesNotThrow(InetAddress::getLocalHost), KafkaPrincipal.ANONYMOUS,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
        Request request = new Request(1, context, 0, MemoryPool.NONE, buffer, requestChannelMetrics);
        quotaManager.throttle(request.header().clientId(), request.session(), channelThrottlingCallback, throttleTimeMs);
    }

}
