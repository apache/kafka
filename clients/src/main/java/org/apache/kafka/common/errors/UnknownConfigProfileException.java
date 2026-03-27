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
package org.apache.kafka.common.errors;

/**
 * Exception thrown when a client's configuration profile is unknown or not supported by the broker.
 * <p>
 * This can occur in two scenarios:
 * <ul>
 *   <li>During GetConfigProfileKeys: The client profile did not match any configuration profiles
 *       and the policy implementation does not allow for that case. This is a fatal error.</li>
 *   <li>During PushConfig: The client sent a request with an invalid or outdated configuration profile CRC,
 *       which means the configuration profile has changed. The client should retry the handshake.</li>
 * </ul>
 */
public class UnknownConfigProfileException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public UnknownConfigProfileException(String message) {
        super(message);
    }

    public UnknownConfigProfileException(String message, Throwable cause) {
        super(message, cause);
    }
}
