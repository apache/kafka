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
 * The request contained a leader epoch which is larger than that on the broker that received the
 * request. This can happen if the client observes a metadata update before it has been propagated
 * to all brokers. Clients need not refresh metadata before retrying.
 */
public class UnknownLeaderEpochException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public UnknownLeaderEpochException(String message) {
        super(message);
    }

    public UnknownLeaderEpochException(String message, Throwable cause) {
        super(message, cause);
    }

}
