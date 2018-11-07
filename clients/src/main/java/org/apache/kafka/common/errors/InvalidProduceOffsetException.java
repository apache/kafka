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
 * Thrown when the offset specified in a Produce request is smaller than the current Log End Offset
 * @see org.apache.kafka.clients.producer.ProducerRecordWithOffset
 */
public class InvalidProduceOffsetException extends InvalidOffsetException {

    private static final long serialVersionUID = 1L;

    private final long logEndOffset;

    public InvalidProduceOffsetException(String message) {
        this(message, -1L);
    }

    public InvalidProduceOffsetException(String message, long logEndOffset) {
        super(message);
        this.logEndOffset = logEndOffset;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }
}
