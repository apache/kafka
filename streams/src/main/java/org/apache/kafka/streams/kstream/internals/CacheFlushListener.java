/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

/**
 * Listen to cache flush events so they can be forwarded to
 * downstream processors.
 * @param <K>
 * @param <V>
 */
public interface CacheFlushListener<K, V> {

    void flushed(final K key, final Change<V> value, final RecordContext recordContext, final InternalProcessorContext context);
}
