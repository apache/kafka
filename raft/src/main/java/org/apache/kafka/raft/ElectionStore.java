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
package org.apache.kafka.raft;

import java.io.IOException;

public interface ElectionStore {

    /**
     * Read the latest election state.
     * @return The latest written election state or `ElectionState.withUnknownLeader(0)` if there is none.
     * @throws IOException For any error encountered reading from the storage
     */
    ElectionState read() throws IOException;

    /**
     * Persist the updated election state. This must be atomic, both writing the full updated state
     * and replacing the old state.
     * @param latest The latest election state
     * @throws IOException For any error encountered while writing the updated state
     */
    void write(ElectionState latest) throws IOException;
}
