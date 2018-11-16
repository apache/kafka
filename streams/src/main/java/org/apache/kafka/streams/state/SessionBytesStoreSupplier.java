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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.Bytes;

/**
 * A store supplier that can be used to create one or more {@link SessionStore SessionStore&lt;Byte, byte[]&gt;} instances.
 *
 * For any stores implementing the {@link SessionStore SessionStore&lt;Byte, byte[]&gt;} interface, {@code null} value
 * bytes are considered as "not exist". This means:
 * <ol>
 *   <li>{@code null} value bytes in put operations should be treated as delete.</li>
 *   <li>{@code null} value bytes should never be returned in range query results.</li>
 * </ol>
 */
public interface SessionBytesStoreSupplier extends StoreSupplier<SessionStore<Bytes, byte[]>> {

    /**
     * The size of a segment, in milliseconds. Used when caching is enabled to segment the cache
     * and reduce the amount of data that needs to be scanned when performing range queries.
     *
     * @return segmentInterval in milliseconds
     */
    long segmentIntervalMs();

    /**
     * The time period for which the {@link SessionStore} will retain historic data.
     *
     * @return retentionPeriod
     */
    long retentionPeriod();
}