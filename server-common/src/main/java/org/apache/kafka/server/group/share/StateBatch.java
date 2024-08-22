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

package org.apache.kafka.server.group.share;

/**
 * Interface to be implement by share group/coordinator classes
 * which represent a batch (KIP-932).
 */
public interface StateBatch {
    /**
     * First offset or start offset of a batch.
     *
     * @return long representing the offset
     */
    long firstOffset();

    /**
     * Last offset or start offset of a batch.
     *
     * @return long representing the offset
     */
    long lastOffset();

    /**
     * The state in which these offsets are.
     *
     * @return byte representing delivery state
     */
    byte deliveryState();

    /**
     * Number of times this is batch is delivered
     *
     * @return short representing delivery count
     */
    short deliveryCount();
}
