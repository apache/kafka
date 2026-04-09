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
package org.apache.kafka.streams.state.internals;

/**
 * Marker interface for time-ordered stores that support optional indexing.
 * <p>
 * Time-ordered stores maintain data in time order and may optionally have an index
 * for efficient time-based queries. The {@link #hasIndex()} method indicates whether
 * the store has been configured with an index.
 */
public interface TimeOrderedStore {
    /**
     * Indicates whether this store has an index for efficient time-ordered queries.
     *
     * @return true if this store has an index, false otherwise
     */
    boolean hasIndex();
}
