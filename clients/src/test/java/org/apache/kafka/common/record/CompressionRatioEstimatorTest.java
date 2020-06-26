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
import java.util.Arrays;
import java.util.List;

public class CompressionRatioEstimatorTest {

    @Test
    public void testUpdateEstimation() {
        class EstimationsObservedRatios {
            float currentEstimation;
            float observedRatio;
            EstimationsObservedRatios(float currentEstimation, float observedRatio) {
                this.currentEstimation = currentEstimation;
                this.observedRatio = observedRatio;
            }
        }

        // If currentEstimation is smaller than observedRatio, the updatedCompressionRatio is currentEstimation plus
        // COMPRESSION_RATIO_DETERIORATE_STEP 0.05, otherwise currentEstimation minus COMPRESSION_RATIO_IMPROVING_STEP
        // 0.005. There are four cases,and updatedCompressionRatio shouldn't smaller than observedRatio in all of cases.
        // Refer to non test code for more details.
        List<EstimationsObservedRatios> estimationsObservedRatios = Arrays.asList(
            new EstimationsObservedRatios(0.8f, 0.84f),
            new EstimationsObservedRatios(0.6f, 0.7f),
            new EstimationsObservedRatios(0.6f, 0.4f),
            new EstimationsObservedRatios(0.004f, 0.001f));
        for (EstimationsObservedRatios estimationsObservedRatio : estimationsObservedRatios) {
            String topic = "tp";
            CompressionRatioEstimator.setEstimation(topic, CompressionType.ZSTD, estimationsObservedRatio.currentEstimation);
            float updatedCompressionRatio = CompressionRatioEstimator.updateEstimation(topic, CompressionType.ZSTD, estimationsObservedRatio.observedRatio);
            assertTrue(updatedCompressionRatio >= estimationsObservedRatio.observedRatio);
        }
    }
}
