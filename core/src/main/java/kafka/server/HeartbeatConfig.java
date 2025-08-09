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

/**
 * Configuration for broker heartbeat behavior during metadata loading operations.
 * This helps prevent broker fencing during rolling restarts with heavy metadata replication.
 */
public class HeartbeatConfig {
    
    /**
     * Maximum time to extend heartbeat timeout during metadata loading (in milliseconds).
     * Default: 30000ms (30 seconds)
     */
    public static final long DEFAULT_METADATA_LOADING_TIMEOUT_EXTENSION_MS = 30000L;
    
    /**
     * Maximum number of batches to process before yielding control to heartbeat operations.
     * Default: 10 batches
     */
    public static final int DEFAULT_MAX_BATCHES_PER_ITERATION = 10;
    
    /**
     * Maximum time to spend processing metadata before yielding control (in milliseconds).
     * Default: 50ms
     */
    public static final long DEFAULT_MAX_PROCESSING_TIME_MS = 50L;
    
    /**
     * Maximum number of publishers to process before yielding control.
     * Default: 5 publishers
     */
    public static final int DEFAULT_PUBLISHER_BATCH_SIZE = 5;
    
    /**
     * Maximum time to spend processing publishers before yielding control (in milliseconds).
     * Default: 100ms
     */
    public static final long DEFAULT_MAX_PUBLISHER_PROCESSING_TIME_MS = 100L;
}