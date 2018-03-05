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
    private Random random = null;

    public Payload() {
        this(DEFAULT_MESSAGE_SIZE, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    public Payload(Integer messageSize) {
        this(messageSize, DEFAULT_VALUE_DIVERGENCE_RATIO);
    }

    /**
     * @param messageSize key + value size
     * @param valueDivergenceRatio ratio of how much each next value is different from the previous
     *                             value. Used to approximately control target compression rate (if
     *                             compression is used).
     */
    public Payload(Integer messageSize, double valueDivergenceRatio) {
        this.valueDivergenceRatio = valueDivergenceRatio;
        this.random = new Random();
        this.recordValue = new byte[messageSize];
        // initialize value with random bytes
        for (int i = 0; i < recordValue.length; ++i) {
            recordValue[i] = (byte) (this.random.nextInt(26) + 65);
        }
    }

    /**
     * Returns producer record value
     */
    public byte[] nextValue() {
        // randomize some of the payload to achieve expected compression rate
        for (int i = 0; i < recordValue.length * valueDivergenceRatio; ++i)
            recordValue[i] = (byte) (this.random.nextInt(26) + 65);
        return recordValue;
    }

    /**
     * Returns producer record key
     */
    public byte[] nextKey() {
        return null;
    }

    @Override
    public String toString() {
        return "Payload(recordKeySize=0" + ", recordValueSize=" + recordValue.length
               + ", valueDivergenceRatio=" + valueDivergenceRatio + ")";
    }
}
