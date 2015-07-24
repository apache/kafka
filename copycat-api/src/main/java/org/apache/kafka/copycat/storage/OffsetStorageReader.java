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
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.copycat.data.Schema;

import java.util.Collection;
import java.util.Map;

/**
 * OffsetStorageReader provides access to the offset storage used by sources. This can be used by
 * connectors to determine offsets to start consuming data from. This is most commonly used during
 * initialization of a task, but can also be used during runtime, e.g. when reconfiguring a task.
 */
public interface OffsetStorageReader {
    /**
     * Get the offset for the specified stream. If the data isn't already available locally, this
     * gets it from the backing store, which may require some network round trips.
     *
     * @param stream object uniquely identifying the stream of data
     * @param schema schema used to decode the offset value
     * @return object uniquely identifying the offset in the stream of data
     */
    public Object getOffset(Object stream, Schema schema);

    /**
     * <p>
     * Get a set of offsets for the specified stream identifiers. This may be more efficient
     * than calling {@link #getOffset(Object, Schema)} repeatedly.
     * </p>
     * <p>
     * Note that when errors occur, this method omits the associated data and tries to return as
     * many of the requested values as possible. This allows a task that's managing many streams to
     * still proceed with any available data. Therefore, implementations should take care to check
     * that the data is actually available in the returned response. The only case when an
     * exception will be thrown is if the entire request failed, e.g. because the underlying
     * storage was unavailable.
     * </p>
     *
     * @param streams set of identifiers for streams of data
     * @param schema schema used to decode offset values
     * @return a map of stream identifiers to decoded offsets
     */
    public Map<Object, Object> getOffsets(Collection<Object> streams, Schema schema);
}
