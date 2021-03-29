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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.BytesComparators;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksIterator;
import java.util.Set;

public class RocksDBPrefixRangeIterator extends RocksDBRangeIterator {
    RocksDBPrefixRangeIterator(final String storeName,
                               final RocksIterator iter,
                               final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
                               final Bytes from,
                               final Bytes to,
                               final boolean forward,
                               final boolean toInclusive) {
        super(storeName,
            iter,
            openIterators,
            from,
            to,
            forward,
            toInclusive,
            BytesComparators.PREFIX_BYTES_LEXICO_COMPARATOR);
    }
}
