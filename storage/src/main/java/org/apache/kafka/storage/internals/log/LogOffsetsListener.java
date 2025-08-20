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
package org.apache.kafka.storage.internals.log;

/**
 * Listener receive notification from the Log.
 *
 * Note that the callbacks are executed in the thread that triggers the change
 * AND that locks may be held during their execution. They are meant to be used
 * as notification mechanism only.
 */
public interface LogOffsetsListener {
    /**
     * A default no op offsets listener.
     */
    LogOffsetsListener NO_OP_OFFSETS_LISTENER = new LogOffsetsListener() { };

    /**
     * Called when the Log increments its high watermark.
     */
    default void onHighWatermarkUpdated(long offset) {}
}
