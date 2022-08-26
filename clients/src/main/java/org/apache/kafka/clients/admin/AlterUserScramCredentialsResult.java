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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The result of the {@link Admin#alterUserScramCredentials(List)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AlterUserScramCredentialsResult {
    private final Map<String, KafkaFuture<Void>> futures;

    /**
     *
     * @param futures the required map from user names to futures representing the results of the alteration(s)
     *                for each user
     */
    public AlterUserScramCredentialsResult(Map<String, KafkaFuture<Void>> futures) {
        this.futures = Collections.unmodifiableMap(Objects.requireNonNull(futures));
    }

    /**
     * Return a map from user names to futures, which can be used to check the status of the alteration(s)
     * for each user.
     */
    public Map<String, KafkaFuture<Void>> values() {
        return this.futures;
    }

    /**
     * Return a future which succeeds only if all the user SCRAM credential alterations succeed.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
