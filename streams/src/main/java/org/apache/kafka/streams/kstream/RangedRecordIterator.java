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
package org.apache.kafka.streams.kstream;

import java.util.Iterator;

/**
 * An {@link Iterator} that holds resources (e.g. a RocksDB cursor) and must be closed after use.
 * The {@link #close()} method does not throw a checked exception, making it safe for use in
 * try-with-resources blocks without requiring a catch clause.
 *
 * <p>Implementations must be idempotent: calling {@link #close()} more than once must be safe.
 *
 * @param <T> the type of elements returned by this iterator
 */
public interface RangedRecordIterator<T> extends Iterator<T>, AutoCloseable {
    @Override
    void close();
}
