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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.errors.TimeoutException;

/**
 * This exception is thrown if the producer cannot allocate memory for a record within max.block.ms due to the buffer
 * being too full.
 *
 * In earlier versions a TimeoutException was thrown instead of this. To keep existing catch-clauses working
 * this class extends TimeoutException.
 *
 */
public class BufferExhaustedException extends TimeoutException {

    private static final long serialVersionUID = 1L;

    public BufferExhaustedException(String message) {
        super(message);
    }

}
