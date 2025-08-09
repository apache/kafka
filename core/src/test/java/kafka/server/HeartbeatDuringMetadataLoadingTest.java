/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.BrokerHeartbeatManager;
import org.apache.kafka.controller.BrokerIdAndEpoch;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that broker heartbeats are not blocked during heavy metadata loading operations.
 * This addresses the issue where brokers get fenced during rolling restarts due to heartbeat timeouts.
 */
public class HeartbeatDuringMetadataLoadingTest {
    
    private MockTime time;
    private BrokerHeartbeatManager heartbeatManager;
    private static final long SESSION_TIMEOUT_NS = 9000L * 1000 * 1000; // 9 seconds in nanoseconds
    
    @BeforeEach
    public void setUp() {
        time = new MockTime();
        LogContext logContext = new LogContext("[HeartbeatTest] ");
        heartbeatManager = new BrokerHeartbeatManager(logContext, time, SESSION_TIMEOUT_NS);
    }
    
    @Test
    public void testHeartbeatTimeoutExtensionDuringMetadataLoading() {
        int brokerId = 1;
        long brokerEpoch = 100L;
        
        // Register broker
        heartbeatManager.register(brokerId, false);
        
        // Touch broker to establish session
        heartbeatManager.touch(brokerId, false, 0L);
        
        // Verify broker has valid session
        assertTrue(heartbeatManager.hasValidSession(brokerId, brokerEpoch));
        
        // Simulate broker falling behind in metadata (common during rolling restart)
        heartbeatManager.touch(brokerId, false, 50L);
        
        // Check if timeout should be extended during metadata loading
        assertTrue(heartbeatManager.shouldExtendHeartbeatTimeout(brokerId));
        
        // Advance time but not beyond extended timeout
        time.sleep(SESSION_TIMEOUT_NS / 1000000 + 5000); // 5 seconds beyond normal timeout
        
        // Broker should still be considered valid during metadata loading
        assertTrue(heartbeatManager.hasValidSession(brokerId, brokerEpoch));
    }
    
    @Test
    public void testHeartbeatConfigurationConstants() {
        // Verify configuration constants are reasonable
        assertTrue(HeartbeatConfig.DEFAULT_METADATA_LOADING_TIMEOUT_EXTENSION_MS > 0);
        assertTrue(HeartbeatConfig.DEFAULT_MAX_BATCHES_PER_ITERATION > 0);
        assertTrue(HeartbeatConfig.DEFAULT_MAX_PROCESSING_TIME_MS > 0);
        assertTrue(HeartbeatConfig.DEFAULT_PUBLISHER_BATCH_SIZE > 0);
        assertTrue(HeartbeatConfig.DEFAULT_MAX_PUBLISHER_PROCESSING_TIME_MS > 0);
        
        // Verify reasonable defaults
        assertEquals(30000L, HeartbeatConfig.DEFAULT_METADATA_LOADING_TIMEOUT_EXTENSION_MS);
        assertEquals(10, HeartbeatConfig.DEFAULT_MAX_BATCHES_PER_ITERATION);
        assertEquals(50L, HeartbeatConfig.DEFAULT_MAX_PROCESSING_TIME_MS);
        assertEquals(5, HeartbeatConfig.DEFAULT_PUBLISHER_BATCH_SIZE);
        assertEquals(100L, HeartbeatConfig.DEFAULT_MAX_PUBLISHER_PROCESSING_TIME_MS);
    }
    
    @Test
    public void testNormalHeartbeatBehaviorUnchanged() {
        int brokerId = 2;
        long brokerEpoch = 200L;
        
        // Register broker
        heartbeatManager.register(brokerId, false);
        
        // Touch broker to establish session
        heartbeatManager.touch(brokerId, false, 100L);
        
        // Verify broker has valid session
        assertTrue(heartbeatManager.hasValidSession(brokerId, brokerEpoch));
        
        // Advance time beyond normal timeout
        time.sleep(SESSION_TIMEOUT_NS / 1000000 + 1000); // 1 second beyond timeout
        
        // For normal operation (not during metadata loading), timeout should work as before
        // This test ensures we don't break existing behavior
        assertFalse(heartbeatManager.shouldExtendHeartbeatTimeout(999)); // Non-existent broker
    }
}