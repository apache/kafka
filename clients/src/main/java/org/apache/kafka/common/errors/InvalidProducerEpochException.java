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
 * This exception indicates that the produce request sent to the partition leader
 * contains a non-matching producer epoch. When encountering this exception, user should abort the ongoing transaction
 * by calling KafkaProducer#abortTransaction which would try to send initPidRequest and reinitialize the producer
 * under the hood.
 */
public class InvalidProducerEpochException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidProducerEpochException(String message) {
        super(message);
    }
}
