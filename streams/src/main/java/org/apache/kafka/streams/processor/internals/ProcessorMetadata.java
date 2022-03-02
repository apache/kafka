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
package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

/**
 * ProcessorMetadata to be access and populated by processor node. This will be committed along with
 * offset
 */
public class ProcessorMetadata {

    // Does this need to be thread safe? I think not since there's one per task
    private final Map<Bytes, byte[]> globalMetadata;

    public ProcessorMetadata() {
        globalMetadata = new HashMap<>();
    }

    public static ProcessorMetadata deserialize(final byte[] ProcessorMetadata, final TopicPartition partition) {
        // TODO: deserialize
        return null;
    }

    public void merge(final ProcessorMetadata other) {
        // TODO: merge with other data
    }

    public byte[] serialize(final TopicPartition partition) {
        // TODO: serialize for partition
        return null;
    }

    public void addGlobalMetadata(final Bytes key, final byte[] value) {
      globalMetadata.put(key, value);
    }

    public byte[] getGlobalMetadata(final Bytes key) {
      return globalMetadata.get(key);
    }

}
