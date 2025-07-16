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
package org.apache.kafka.connect.runtime;

import java.util.Objects;

import javax.crypto.SecretKey;

/**
 * A session key, which can be used to validate internal REST requests between workers.
 * @param key               the actual cryptographic key to use for request validation; may not be null
 * @param creationTimestamp the time at which the key was generated
 */
public record SessionKey(SecretKey key, long creationTimestamp) {

    public SessionKey {
        Objects.requireNonNull(key, "Key may not be null");
    }
}
