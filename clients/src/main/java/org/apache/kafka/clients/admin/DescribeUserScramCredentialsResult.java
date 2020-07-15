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

import java.util.Map;
import java.util.Objects;

/**
 * The result of the {@link Admin#describeUserScramCredentials()} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeUserScramCredentialsResult {
    private final KafkaFuture<Map<String, UserScramCredentialsDescription>> future;

    /**
     *
     * @param future the required future representing the result of the call
     */
    public DescribeUserScramCredentialsResult(KafkaFuture<Map<String, UserScramCredentialsDescription>> future) {
        this.future = Objects.requireNonNull(future);
    }

    /**
     *
     * @return the future representing the result of the call
     */
    public KafkaFuture<Map<String, UserScramCredentialsDescription>> all() {
        return this.future;
    }
}
