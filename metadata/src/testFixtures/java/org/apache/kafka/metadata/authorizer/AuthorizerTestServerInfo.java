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

package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

public record AuthorizerTestServerInfo(Collection<Endpoint> endpoints) implements AuthorizerServerInfo {
    public AuthorizerTestServerInfo {
        assertFalse(endpoints.isEmpty());
    }

    @Override
    public ClusterResource clusterResource() {
        return new ClusterResource(Uuid.fromString("r7mqHQrxTNmzbKvCvWZzLQ").toString());
    }

    @Override
    public int brokerId() {
        return 0;
    }

    @Override
    public Endpoint interBrokerEndpoint() {
        return endpoints.iterator().next();
    }

    @Override
    public Collection<String> earlyStartListeners() {
        List<String> result = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            if (endpoint.listener().equals("CONTROLLER")) {
                result.add(endpoint.listener());
            }
        }
        return result;
    }
}
