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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectUtilsTest {

    @Test
    public void testLookupKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1);

        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupNullKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

        assertNull(ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test(expected = ConnectException.class)
    public void testLookupKafkaClusterIdTimeout() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1);
        adminClient.timeoutNextRequest(1);

        ConnectUtils.lookupKafkaClusterId(adminClient);
    }

}
