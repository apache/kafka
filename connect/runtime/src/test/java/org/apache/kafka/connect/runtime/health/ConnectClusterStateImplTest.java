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
package org.apache.kafka.connect.runtime.health;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.HerderProvider;
import org.apache.kafka.connect.util.Callback;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class ConnectClusterStateImplTest {
  
    protected static final long CONNECTORS_TIMEOUT_MS = 2_000;
    
    protected Herder mockHerder;
    protected HerderProvider herderProvider;
    protected ConnectClusterStateImpl connectClusterState;
    protected Collection<String> expectedConnectors;
    
    @Before
    public void setUp() {
        mockHerder = EasyMock.mock(Herder.class);
        herderProvider = new HerderProvider(mockHerder);
        connectClusterState = new ConnectClusterStateImpl(herderProvider, CONNECTORS_TIMEOUT_MS);
        expectedConnectors = Arrays.asList("sink1", "source1", "source2");
    }
    
    @Test
    public void connectors() {
        mockHerder.connectors(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer() throws Throwable {
                Callback<Collection<String>> connectorsCallback =
                    (Callback<Collection<String>>) EasyMock.getCurrentArguments()[0];
                connectorsCallback.onCompletion(null, expectedConnectors);
                return null;
            }
        });
        EasyMock.replay(mockHerder);
        
        assertEquals(expectedConnectors, connectClusterState.connectors());
    }
    
    @Test(expected = ConnectException.class)
    public void connectorsFailure() {
        mockHerder.connectors(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            @SuppressWarnings("unchecked")
            public Void answer() throws Throwable {
                Callback<Collection<String>> connectorsCallback =
                    (Callback<Collection<String>>) EasyMock.getCurrentArguments()[0];
                connectorsCallback.onCompletion(new RuntimeException(), null);
                return null;
            }
        });
        EasyMock.replay(mockHerder);
        
        connectClusterState.connectors();
    }
}
