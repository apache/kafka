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

public class RocksDbTimeOrderedKeyValueBytesStoreSupplier {
  private final String name;
  private final long retentionPeriod;
  private final boolean withIndex;

  public RocksDbTimeOrderedKeyValueBytesStoreSupplier(final String name,
                                                      final long retentionPeriod,
                                                      final boolean withIndex) {
    this.name = name + "-buffer";
    this.retentionPeriod = retentionPeriod;
    this.withIndex = withIndex;
  }

  public String name() {
    return name;
  }

  public RocksDBTimeOrderedKeyValueSegmentedBytesStore get() {
    return new RocksDBTimeOrderedKeyValueSegmentedBytesStore(
            name,
            metricsScope(),
            retentionPeriod,
            Math.max(retentionPeriod / 2, 60_000L),
            withIndex
        );
  }

  public String metricsScope() {
    return "rocksdb-session";
  }

}