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

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class CompressionRatioEstimatorTest {
    @Test
    public void testUpdateEstimation() {
        String topic = "tp";
        class EstimationsObservedRatios {
            float currentEstimation;
            float observedRatio;
            float expected;
            EstimationsObservedRatios(float currentEstimation, float observedRatio, float expected) {
                this.currentEstimation = currentEstimation;
                this.observedRatio = observedRatio;
                this.expected = expected;
            }
        }

        // Method updateEstimation is to update compressionRatioForTopic according to observedRatio and currentEstimation.
        // If currentEstimation is smaller than observedRatio, update compressionRatioForTopic to
        // Math.max(currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP, observedRatio)
        // If currentEstimation is larger than observedRatio, update compressionRatioForTopic to
        // Math.max(currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP, observedRatio).
        // COMPRESSION_RATIO_DETERIORATE_STEP is 0.05f. COMPRESSION_RATIO_IMPROVING_STEP is 0.005f.
        // There are four cases:
        // 1. currentEstimation < observedRatio && (currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP) < observedRatio
        // 2. currentEstimation < observedRatio && (currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP) > observedRatio
        // 3. currentEstimation > observedRatio && (currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP) > observedRatio
        // 4. currentEstimation > observedRatio && (currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP) < observedRatio
        // In all cases, updatedCompressionRatio shouldn't smaller than observedRatio
        EstimationsObservedRatios[] currentEstimationsObservedRatios = new EstimationsObservedRatios[] {
            new EstimationsObservedRatios(0.8f, 0.84f, 0.84f),
            new EstimationsObservedRatios(0.6f, 0.7f, 0.7f),
            new EstimationsObservedRatios(0.6f, 0.4f, 0.4f),
            new EstimationsObservedRatios(0.004f, 0.001f, 0.001f)
        };

        float updatedCompressionRatio;
        for (int i = 0; i < currentEstimationsObservedRatios.length; i++) {
            CompressionRatioEstimator.setEstimation(topic, CompressionType.ZSTD, currentEstimationsObservedRatios[i].currentEstimation);
            updatedCompressionRatio = CompressionRatioEstimator.updateEstimation(topic, CompressionType.ZSTD, currentEstimationsObservedRatios[i].observedRatio);
            assertTrue(updatedCompressionRatio >= currentEstimationsObservedRatios[i].expected);
        }
    }
}
