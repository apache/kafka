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

import org.apache.kafka.streams.query.Position;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public final class PositionSerde {

    // uninstantiable utility class
    public PositionSerde() {
    }

    public static Position deserialize(final ByteBuffer buffer) {
        final byte version = buffer.get();

        switch (version) {
            case (byte) 0: // actual position, v0
                final int nTopics = buffer.getInt();
                final Map<String, Map<Integer, Long>> position = new HashMap<>(nTopics);
                for (int i = 0; i < nTopics; i++) {
                    final int topicNameLength = buffer.getInt();
                    final byte[] topicNameBytes = new byte[topicNameLength];
                    buffer.get(topicNameBytes);
                    final String topic = new String(topicNameBytes, StandardCharsets.UTF_8);

                    final int numPairs = buffer.getInt();
                    final Map<Integer, Long> partitionOffsets = new HashMap<>(numPairs);
                    for (int j = 0; j < numPairs; j++) {
                        partitionOffsets.put(buffer.getInt(), buffer.getLong());
                    }
                    position.put(topic, partitionOffsets);
                }
                return Position.fromMap(position);
            default:
                throw new IllegalArgumentException(
                        "Unknown version " + version + " when deserializing Position"
                );
        }
    }

    public static ByteBuffer serialize(final Position position) {
        final byte version = (byte) 0;

        int arraySize = Byte.SIZE; // version

        final int nTopics = position.getTopics().size();
        arraySize += Integer.SIZE;

        final ArrayList<String> entries =
                new ArrayList<>(position.getTopics());
        final byte[][] topics = new byte[entries.size()][];

        for (int i = 0; i < nTopics; i++) {
            final String topic = entries.get(i);
            final byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            topics[i] = topicBytes;
            arraySize += Integer.SIZE; // topic name length
            arraySize += topicBytes.length; // topic name itself

            final Map<Integer, Long> partitionOffsets = position.getPartitionPositions(topic);
            arraySize += Integer.SIZE; // Number of PartitionOffset pairs
            arraySize += (Integer.SIZE + Long.SIZE)
                    * partitionOffsets.size(); // partitionOffsets themselves
        }

        final ByteBuffer buffer = ByteBuffer.allocate(arraySize);
        buffer.put(version);

        buffer.putInt(nTopics);
        for (int i = 0; i < nTopics; i++) {
            buffer.putInt(topics[i].length);
            buffer.put(topics[i]);

            final String topic = entries.get(i);
            final Map<Integer, Long> partitionOffsets = position.getPartitionPositions(topic);
            buffer.putInt(partitionOffsets.size());
            for (final Entry<Integer, Long> partitionOffset : partitionOffsets.entrySet()) {
                buffer.putInt(partitionOffset.getKey());
                buffer.putLong(partitionOffset.getValue());
            }
        }

        buffer.flip();
        return buffer;
    }
}
