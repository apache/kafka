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
package org.apache.kafka.tiered.storage.utils;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageTraverser;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.SEGMENT;

public final class LocalTieredStorageOutput<K, V> implements LocalTieredStorageTraverser {
    private final Deserializer<K> keyDe;
    private final Deserializer<V> valueDe;
    private String output = row("File", "Offsets", "Records", "Broker ID");
    private String currentTopic = "";

    public LocalTieredStorageOutput(Deserializer<K> keyDe, Deserializer<V> valueDe) {
        this.keyDe = keyDe;
        this.valueDe = valueDe;
        // Columns length + 5 column separators.
        output += repeatString("-", 51 + 8 + 13 + 10 + (3 * 2)) + System.lineSeparator();
    }

    private String row(String file, Object offset, String record, String ident) {
        return String.format("%-51s |%8s |%13s %n", ident + file, offset.toString(), record);
    }

    private String row(String file, Object offset, String record) {
        return row(file, offset, record, "    ");
    }

    private String row() {
        return row("", "", "");
    }

    private String repeatString(String str, int times) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < times; i++) {
            builder.append(str);
        }
        return builder.toString();
    }

    @Override
    public void visitTopicIdPartition(TopicIdPartition topicIdPartition) {
        currentTopic = topicIdPartition.topicPartition().topic();
        output += row(topicIdPartition.topicPartition().toString(), "", "", "");
    }

    @Override
    public void visitSegment(RemoteLogSegmentFileset fileset) {
        try {
            List<Record> records = fileset.getRecords();
            String segFilename = fileset.getFile(SEGMENT).getName();
            if (records.isEmpty()) {
                output += row(segFilename, -1, "");
            } else {
                List<Tuple2<Long, String>> offsetKeyValues = records
                        .stream()
                        .map(record -> new Tuple2<>(record.offset(),
                                "(" + des(keyDe, record.key()) + ", " + des(valueDe, record.value()) + ")"))
                        .collect(Collectors.toList());
                output += row(segFilename, offsetKeyValues.get(0).t1, offsetKeyValues.get(0).t2);
                if (offsetKeyValues.size() > 1) {
                    offsetKeyValues.subList(1, records.size()).forEach(offsetKeyValue ->
                            output += row("", offsetKeyValue.t1, offsetKeyValue.t2));
                }
            }
            output += row();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getOutput() {
        return output;
    }

    private String des(Deserializer<?> de, ByteBuffer bytes) {
        return de.deserialize(currentTopic, Utils.toNullableArray(bytes)).toString();
    }

    private static class Tuple2<T1, T2> {
        private final T1 t1;
        private final T2 t2;

        Tuple2(T1 t1, T2 t2) {
            this.t1 = t1;
            this.t2 = t2;
        }
    }
}