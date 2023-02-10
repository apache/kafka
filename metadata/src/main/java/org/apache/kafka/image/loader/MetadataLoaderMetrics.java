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

package org.apache.kafka.image.loader;

import org.apache.kafka.image.MetadataProvenance;


/**
 * An interface for the metadata loader metrics.
 */
public interface MetadataLoaderMetrics extends AutoCloseable {
    /**
     * Update the batch processing time histogram.
     */
    void updateBatchProcessingTime(long elapsedNs);

    /**
     * Update the batch size histogram.
     */
    void updateBatchSize(int size);

    /**
     * Set the provenance of the last image which has been processed by all publishers.
     */
    void updateLastAppliedImageProvenance(MetadataProvenance provenance);

    /**
     * Retrieve the last offset which has been processed by all publishers.
     */
    long lastAppliedOffset();
}
