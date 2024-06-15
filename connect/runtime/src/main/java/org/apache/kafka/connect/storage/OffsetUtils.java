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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.util.ConnectUtils.className;

public class OffsetUtils {

    private static final Logger log = LoggerFactory.getLogger(OffsetUtils.class);

    @SuppressWarnings("unchecked")
    public static void validateFormat(Object offsetData) {
        if (offsetData == null)
            return;

        if (!(offsetData instanceof Map))
            throw new DataException("Offsets must be specified as a Map");
        validateFormat((Map<Object, Object>) offsetData);
    }

    public static <K, V> void validateFormat(Map<K, V> offsetData) {
        // Both keys and values for offsets may be null. For values, this is a useful way to delete offsets or indicate
        // that there's not usable concept of offsets in your source system.
        if (offsetData == null)
            return;

        for (Map.Entry<K, V> entry : offsetData.entrySet()) {
            if (!(entry.getKey() instanceof String))
                throw new DataException("Offsets may only use String keys");

            Object value = entry.getValue();
            if (value == null)
                continue;
            Schema.Type schemaType = ConnectSchema.schemaType(value.getClass());
            if (schemaType == null)
                throw new DataException("Offsets may only contain primitive types as values, but field " + entry.getKey() + " contains " + value.getClass());
            if (!schemaType.isPrimitive())
                throw new DataException("Offsets may only contain primitive types as values, but field " + entry.getKey() + " contains " + schemaType);
        }
    }

    /**
     * Parses a partition key that is read back from an offset backing store and adds / removes the partition in the
     * provided {@code connectorPartitions} map. If the partition key has an unexpected format, a warning log is emitted
     * and nothing is added / removed in the {@code connectorPartitions} map.
     * @param partitionKey the partition key to be processed
     * @param offsetValue the offset value corresponding to the partition key; determines whether the partition should
     *                    be added to the {@code connectorPartitions} map or removed depending on whether the offset
     *                    value is null or not
     * @param keyConverter the key converter to deserialize the partition key
     * @param connectorPartitions the map from connector names to its set of partitions which needs to be updated after
     *                            processing the partition key
     */
    @SuppressWarnings("unchecked")
    public static void processPartitionKey(byte[] partitionKey, byte[] offsetValue, Converter keyConverter,
                                           Map<String, Set<Map<String, Object>>> connectorPartitions) {

        // The key is expected to always be of the form [connectorName, partition] where connectorName is a
        // string value and partition is a Map<String, Object>

        if (partitionKey == null) {
            log.warn("Ignoring offset partition key with an unexpected null value");
            return;
        }
        // The topic parameter is irrelevant for the JsonConverter which is the internal converter used by
        // Connect workers.
        Object deserializedKey;
        try {
            deserializedKey = keyConverter.toConnectData("", partitionKey).value();
        } catch (DataException e) {
            log.warn("Ignoring offset partition key with unknown serialization. Expected json.", e);
            return;
        }
        if (!(deserializedKey instanceof List)) {
            log.warn("Ignoring offset partition key with an unexpected format. Expected type: {}, actual type: {}",
                    List.class.getName(), className(deserializedKey));
            return;
        }

        List<Object> keyList = (List<Object>) deserializedKey;
        if (keyList.size() != 2) {
            log.warn("Ignoring offset partition key with an unexpected number of elements. Expected: 2, actual: {}", keyList.size());
            return;
        }

        if (!(keyList.get(0) instanceof String)) {
            log.warn("Ignoring offset partition key with an unexpected format for the first element in the partition key list. " +
                    "Expected type: {}, actual type: {}", String.class.getName(), className(keyList.get(0)));
            return;
        }

        if (!(keyList.get(1) instanceof Map)) {
            if (keyList.get(1) != null) {
                log.warn("Ignoring offset partition key with an unexpected format for the second element in the partition key list. " +
                        "Expected type: {}, actual type: {}", Map.class.getName(), className(keyList.get(1)));
            }
            return;
        }

        String connectorName = (String) keyList.get(0);
        Map<String, Object> partition = (Map<String, Object>) keyList.get(1);
        connectorPartitions.computeIfAbsent(connectorName, ignored -> new HashSet<>());
        if (offsetValue == null) {
            connectorPartitions.get(connectorName).remove(partition);
        } else {
            connectorPartitions.get(connectorName).add(partition);
        }
    }
}
