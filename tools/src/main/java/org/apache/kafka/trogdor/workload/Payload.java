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
package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Describes the payload for the producer record. Currently, we can configure message size
 * (constant) and approximately control compression ratio. Where value is of message size, and key
 * is null.
 * We will likely make this class a base class in the future and derive different payload classes
 * for various message size distributions, key assignments, etc.
 */
public class Payload {

    public static final double DEFAULT_VALUE_DIVERGENCE_RATIO = 0.3;
    public static final int DEFAULT_MESSAGE_SIZE = 512;

    /**
     * This is the ratio of how much each next value is different from the previous value. This
     * is directly related to compression rate we will get. Example: 0.3 divergence ratio gets us
     * about 0.3 - 0.45 compression rate with lz4.
     */
    private final double valueDivergenceRatio;
    private byte[] recordValue;
    private PayloadKeyType recordKeyType;
    private Random random = null;

    public Payload() {
        this(DEFAULT_MESSAGE_SIZE, PayloadKeyType.KEY_NULL, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    public Payload(Integer messageSize) {
        this(messageSize, PayloadKeyType.KEY_NULL, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    public Payload(Integer messageSize, PayloadKeyType keyType) {
        this(messageSize, keyType, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    /**
     * @param messageSize key + value size
     * @param valueDivergenceRatio ratio of how much each next value is different from the previous
     *                             value. Used to approximately control target compression rate (if
     *                             compression is used).
     */
    public Payload(Integer messageSize, PayloadKeyType keyType, double valueDivergenceRatio) {
        this.valueDivergenceRatio = valueDivergenceRatio;
        this.random = new Random();

        final int valueSize = (messageSize > keyType.maxSizeInBytes())
                              ? messageSize - keyType.maxSizeInBytes() : 1;
        this.recordValue = new byte[valueSize];
        // initialize value with random bytes
        for (int i = 0; i < recordValue.length; ++i) {
            recordValue[i] = (byte) (this.random.nextInt(26) + 65);
        }
        this.recordKeyType = keyType;
    }

    /**
     * Creates record with null key.
     */
    public ProducerRecord<byte[], byte[]> nextRecord(String topicName) {
        if (recordKeyType != PayloadKeyType.KEY_NULL) {
            // we don't know (for currently supported key types) how to create a key
            throw new IllegalArgumentException();
        }
        return new ProducerRecord<>(topicName, null, nextValue());
    }

    /**
     * Creates record with fixed size key containing given integer value.
     */
    public ProducerRecord<byte[], byte[]> nextRecord(String topicName, int key) {
        if (recordKeyType.maxSizeInBytes() <= 0) {
            // must call nextRecord(String topicName)
            throw new IllegalArgumentException();
        }
        byte[] keyBytes = ByteBuffer.allocate(recordKeyType.maxSizeInBytes()).putInt(key).array();
        return new ProducerRecord<>(topicName, keyBytes, nextValue());
    }

    @Override
    public String toString() {
        return "Payload(recordKeySize=" + recordKeyType.maxSizeInBytes()
               + ", recordValueSize=" + recordValue.length
               + ", valueDivergenceRatio=" + valueDivergenceRatio + ")";
    }

    /**
     * Returns producer record value
     */
    private byte[] nextValue() {
        // randomize some of the payload to achieve expected compression rate
        for (int i = 0; i < recordValue.length * valueDivergenceRatio; ++i)
            recordValue[i] = (byte) (this.random.nextInt(26) + 65);
        return recordValue;
    }
}
