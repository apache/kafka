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

package org.apache.kafka.common.record;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * This class help estimate the compression ratio for each topic and compression type combination.
 */
public class CompressionRatioEstimator {
    // The constant speed to increase compression ratio when a batch compresses better than expected.
    public static final float COMPRESSION_RATIO_IMPROVING_STEP = 0.005f;
    // The minimum speed to decrease compression ratio when a batch compresses worse than expected.
    public static final float COMPRESSION_RATIO_DETERIORATE_STEP = 0.05f;
    private static final ConcurrentMap<String, float[]> COMPRESSION_RATIO = new ConcurrentHashMap<>();

    /**
     * Update the compression ratio estimation for a topic and compression type.
     *
     * @param topic         the topic to update compression ratio estimation.
     * @param type          the compression type.
     * @param observedRatio the observed compression ratio.
     * @return the compression ratio estimation after the update.
     */
    public static float updateEstimation(String topic, CompressionType type, float observedRatio) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        float currentEstimation = compressionRatioForTopic[type.id];
        synchronized (compressionRatioForTopic) {
            if (observedRatio > currentEstimation)
                compressionRatioForTopic[type.id] = Math.max(currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP, observedRatio);
            else if (observedRatio < currentEstimation) {
                compressionRatioForTopic[type.id] = currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP;
            }
        }
        return compressionRatioForTopic[type.id];
    }

    /**
     * Get the compression ratio estimation for a topic and compression type.
     */
    public static float estimation(String topic, CompressionType type) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        return compressionRatioForTopic[type.id];
    }

    /**
     * Reset the compression ratio estimation to the initial values for a topic.
     */
    public static void resetEstimation(String topic) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        synchronized (compressionRatioForTopic) {
            for (CompressionType type : CompressionType.values()) {
                compressionRatioForTopic[type.id] = type.rate;
            }
        }
    }

    /**
     * Remove the compression ratio estimation for a topic.
     */
    public static void removeEstimation(String topic) {
        COMPRESSION_RATIO.remove(topic);
    }

    /**
     * Set the compression estimation for a topic compression type combination. This method is for unit test purpose.
     */
    public static void setEstimation(String topic, CompressionType type, float ratio) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        synchronized (compressionRatioForTopic) {
            compressionRatioForTopic[type.id] = ratio;
        }
    }

    private static float[] getAndCreateEstimationIfAbsent(String topic) {
        float[] compressionRatioForTopic = COMPRESSION_RATIO.get(topic);
        if (compressionRatioForTopic == null) {
            compressionRatioForTopic = initialCompressionRatio();
            float[] existingCompressionRatio = COMPRESSION_RATIO.putIfAbsent(topic, compressionRatioForTopic);
            // Someone created the compression ratio array before us, use it.
            if (existingCompressionRatio != null)
                return existingCompressionRatio;
        }
        return compressionRatioForTopic;
    }

    private static float[] initialCompressionRatio() {
        float[] compressionRatio = new float[CompressionType.values().length];
        for (CompressionType type : CompressionType.values()) {
            compressionRatio[type.id] = type.rate;
        }
        return compressionRatio;
    }
}