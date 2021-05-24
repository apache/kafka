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

package org.apache.kafka.controller;

import org.apache.kafka.common.protocol.ApiMessage;

import java.util.Iterator;
import java.util.List;


interface SnapshotReader extends Iterator<List<ApiMessage>>, AutoCloseable {
    /**
     * Returns the snapshot epoch, which is the offset of this snapshot within the log.
     */
    long epoch();

    /**
     * Invoked when the snapshot reader is no longer needed.  This should clean
     * up all reader resources.
     */
    void close();
}
