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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ServerInfo;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EasyMockRunner.class)
public class RootResourceTest extends EasyMockSupport {

    @Mock
    private Herder herder;
    private RootResource rootResource;

    @Before
    public void setUp() {
        rootResource = new RootResource(herder);
    }

    @Test
    public void testRootGet() {
        EasyMock.expect(herder.kafkaClusterId()).andReturn(MockAdminClient.DEFAULT_CLUSTER_ID);

        replayAll();

        ServerInfo info = rootResource.serverInfo();
        assertEquals(AppInfoParser.getVersion(), info.version());
        assertEquals(AppInfoParser.getCommitId(), info.commit());
        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, info.clusterId());

        verifyAll();
    }
}
