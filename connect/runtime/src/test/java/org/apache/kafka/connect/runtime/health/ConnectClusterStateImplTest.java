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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    public void fastConnectors() {
        assertEquals(expectedConnectors, connectorsWithDelay(0, TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void slowConnectors() {
        assertEquals(
                expectedConnectors,
                connectorsWithDelay(CONNECTORS_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS)
        );
    }
    
    @Test(expected = ConnectException.class)
    public void timedOutConnectors() {
        connectorsWithDelay(CONNECTORS_TIMEOUT_MS + 500, TimeUnit.MILLISECONDS);
    }
    
    protected Collection<String> connectorsWithDelay(long delay, TimeUnit timeUnit) {
        mockHerder.connectors(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(new IAnswer<Void>() {
                @Override
                @SuppressWarnings("unchecked")
                public Void answer() {
                    Callback<Collection<String>> connectorsCallback =
                            (Callback<Collection<String>>) EasyMock.getCurrentArguments()[0];
                    new Thread(new DelayedConnectorsReturn(
                            delay,
                            timeUnit,
                            expectedConnectors,
                            connectorsCallback
                    )).start();
                    return null;
                }
        });
        EasyMock.replay(mockHerder);
        return connectClusterState.connectors();
    }
    
    protected static class DelayedConnectorsReturn implements Runnable {
        
        private final long delay;
        private final TimeUnit timeUnit;
        private final Collection<String> connectors;
        private final Callback<Collection<String>> connectorsCallback;

        public DelayedConnectorsReturn(
                long delay,
                TimeUnit timeUnit,
                Collection<String> connectors,
                Callback<Collection<String>> connectorsCallback
        ) {
            this.delay = delay;
            this.timeUnit = timeUnit;
            this.connectors = connectors;
            this.connectorsCallback = connectorsCallback;
        }

        @Override
        public void run() {
            try {
                timeUnit.sleep(delay);
            } catch (InterruptedException e) {
                fail("Interrupted while simulating delay");
            }
            connectorsCallback.onCompletion(null, connectors);
        }
    }
}
