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
package org.apache.kafka.security.authorizer;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.server.ProcessRole;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizerUtilsTest {

    @ParameterizedTest
    @EnumSource(ProcessRole.class)
    public void testCreateAuthorizer(ProcessRole role) throws ClassNotFoundException {
        Map<String, Object> configs = Map.of();
        Metrics metrics = new Metrics();
        assertEquals(1, metrics.metrics().size());
        Plugin<Authorizer> authorizer = AuthorizerUtils.createAuthorizer(
                MonitorableAuthorizer.class.getName(),
                configs,
                metrics,
                ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,
                role.toString());
        assertInstanceOf(MonitorableAuthorizer.class, authorizer.get());
        MonitorableAuthorizer monitorableAuthorizer = (MonitorableAuthorizer) authorizer.get();
        assertTrue(monitorableAuthorizer.configured);
        assertNotNull(monitorableAuthorizer.metricName);
        MetricName metricName = null;
        for (MetricName name : metrics.metrics().keySet()) {
            if (name.name().equals(monitorableAuthorizer.metricName.name())) {
                metricName = name;
            }
        }
        assertNotNull(metricName);
        assertEquals(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,  metricName.tags().get("config"));
        assertEquals(MonitorableAuthorizer.class.getSimpleName(),  metricName.tags().get("class"));
        assertEquals(role.toString(),  metricName.tags().get("role"));
        assertTrue(metricName.tags().entrySet().containsAll(MonitorableAuthorizer.EXTRA_TAGS.entrySet()));
        assertEquals(0, metrics.metric(metricName).metricValue());
        monitorableAuthorizer.authorize(null, null);
        assertEquals(1, metrics.metric(metricName).metricValue());
    }

    public static class MonitorableAuthorizer implements Authorizer, Monitorable {

        private static final LinkedHashMap<String, String> EXTRA_TAGS = new LinkedHashMap<>();
        private final AtomicInteger counter = new AtomicInteger();
        private boolean configured = false;
        private MetricName metricName = null;

        static {
            EXTRA_TAGS.put("t1", "v1");
        }

        @Override
        public void withPluginMetrics(PluginMetrics metrics) {
            assertTrue(configured);
            metricName = metrics.metricName("authorize calls", "Number of times authorize was called", EXTRA_TAGS);
            metrics.addMetric(metricName, (Gauge<Integer>) (config, now) -> counter.get());
        }

        @Override
        public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
            return Map.of();
        }

        @Override
        public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
            counter.incrementAndGet();
            return List.of();
        }

        @Override
        public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
            return List.of();
        }

        @Override
        public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
            return List.of();
        }

        @Override
        public Iterable<AclBinding> acls(AclBindingFilter filter) {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            configured = true;
        }
    }

}
