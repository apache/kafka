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

package org.apache.kafka.metadata.publisher;

import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.function.Consumer;


/**
 * Takes as input a potentially long list of records, and feeds the associated consumer a series
 * of smaller lists of records.
 */
public class BatchSplitter implements Consumer<List<ApiMessageAndVersion>> {
    private final int maxRecordsInBatch;
    private final Consumer<List<ApiMessageAndVersion>> consumer;

    public BatchSplitter(
        int maxRecordsInBatch,
        Consumer<List<ApiMessageAndVersion>> consumer
    ) {
        this.maxRecordsInBatch = maxRecordsInBatch;
        this.consumer = consumer;
    }

    @Override
    public void accept(List<ApiMessageAndVersion> messages) {
        int i = 0;
        while (i < messages.size()) {
            consumer.accept(messages.subList(i, Math.min(i + maxRecordsInBatch, messages.size())));
            i += maxRecordsInBatch;
        }
    }
}
