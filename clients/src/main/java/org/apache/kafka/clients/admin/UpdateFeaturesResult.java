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
package org.apache.kafka.clients.admin;

import java.util.Map;
import org.apache.kafka.common.KafkaFuture;

/**
 * The result of the {@link Admin#updateFeatures(Map, UpdateFeaturesOptions)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
public class UpdateFeaturesResult {
    private final Map<String, KafkaFuture<Void>> futures;

    /**
     * @param futures   a map from feature name to future, which can be used to check the status of
     *                  individual feature updates.
     */
    UpdateFeaturesResult(final Map<String, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    public Map<String, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds if all the feature updates succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
