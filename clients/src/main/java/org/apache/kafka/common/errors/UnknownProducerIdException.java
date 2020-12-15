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
 * This exception is raised by the broker if it could not locate the producer metadata associated with the producerId
 * in question. This could happen if, for instance, the producer's records were deleted because their retention time
 * had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker,
 * and future appends by the producer will return this exception.
 */
public class UnknownProducerIdException extends OutOfOrderSequenceException {

    public UnknownProducerIdException(String message) {
        super(message);
    }

}
