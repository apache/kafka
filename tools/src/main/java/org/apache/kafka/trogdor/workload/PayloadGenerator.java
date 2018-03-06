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
 * Describes the payload for the producer record. Currently, it generates constant size values
 * and either null keys or constant size key (depending on requested key type). The generator
 * is deterministic -- two generator objects created with the same key type, message size, and
 * value divergence ratio (see `valueDivergenceRatio` description) will generate the same sequence
 * of key/value pairs.
 */
public class PayloadGenerator {

    public static final double DEFAULT_VALUE_DIVERGENCE_RATIO = 0.3;
    public static final int DEFAULT_MESSAGE_SIZE = 512;

    /**
     * This is the ratio of how much each next value is different from the previous value. This
     * is directly related to compression rate we will get. Example: 0.3 divergence ratio gets us
     * about 0.3 - 0.45 compression rate with lz4.
     */
    private final double valueDivergenceRatio;
    private final long baseSeed;
    private long currentPosition;
    private byte[] recordValue;
    private PayloadKeyType recordKeyType;

    public PayloadGenerator() {
        this(DEFAULT_MESSAGE_SIZE, PayloadKeyType.KEY_NULL, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    /**
     * Generator will generate null keys and values of size `messageSize`
     * @param messageSize number of bytes used for key + value
     */
    public PayloadGenerator(Integer messageSize) {
        this(messageSize, PayloadKeyType.KEY_NULL, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    /**
     * Generator will generate keys of given type and values of size 'messageSize' - (key size).
     * @param messageSize number of bytes used for key + value
     * @param keyType type of keys generated
     */
    public PayloadGenerator(Integer messageSize, PayloadKeyType keyType) {
        this(messageSize, keyType, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    /**
     * @param messageSize key + value size
     * @param valueDivergenceRatio ratio of how much each next value is different from the previous
     *                             value. Used to approximately control target compression rate (if
     *                             compression is used).
     */
    public PayloadGenerator(Integer messageSize, PayloadKeyType keyType,
                            double valueDivergenceRatio) {
        this.baseSeed = 856;  // some random number, may later let pass seed to constructor
        this.currentPosition = 0;
        this.valueDivergenceRatio = valueDivergenceRatio;

        final int valueSize = (messageSize > keyType.maxSizeInBytes())
                              ? messageSize - keyType.maxSizeInBytes() : 1;
        this.recordValue = new byte[valueSize];
        // initialize value with random bytes
        Random random = new Random(baseSeed);
        for (int i = 0; i < recordValue.length; ++i) {
            recordValue[i] = (byte) (random.nextInt(26) + 65);
        }
        this.recordKeyType = keyType;
    }

    /**
     * Returns current position of the payload generator.
     */
    public long position() {
        return currentPosition;
    }

    /**
     * Creates record based on the current position, and increments current position.
     */
    public ProducerRecord<byte[], byte[]> nextRecord(String topicName) {
        return nextRecord(topicName, currentPosition++);
    }

    /**
     * Creates record based on the given position. Does not change the current position.
     */
    public ProducerRecord<byte[], byte[]> nextRecord(String topicName, long position) {
        byte[] keyBytes = null;
        if (recordKeyType == PayloadKeyType.KEY_MESSAGE_INDEX) {
            keyBytes = ByteBuffer.allocate(recordKeyType.maxSizeInBytes()).putLong(position).array();
        } else if (recordKeyType != PayloadKeyType.KEY_NULL) {
            throw new UnsupportedOperationException(
                "PayloadGenerator does not know how to generate key for key type " + recordKeyType);
        }
        return new ProducerRecord<>(topicName, keyBytes, nextValue(position));
    }

    @Override
    public String toString() {
        return "PayloadGenerator(recordKeySize=" + recordKeyType.maxSizeInBytes()
               + ", recordValueSize=" + recordValue.length
               + ", valueDivergenceRatio=" + valueDivergenceRatio + ")";
    }

    /**
     * Returns producer record value
     */
    private byte[] nextValue(long position) {
        // randomize some of the payload to achieve expected compression rate
        Random random = new Random(baseSeed + 31 * position + 1);
        for (int i = 0; i < recordValue.length * valueDivergenceRatio; ++i)
            recordValue[i] = (byte) (random.nextInt(26) + 65);
        return recordValue;
    }
}
