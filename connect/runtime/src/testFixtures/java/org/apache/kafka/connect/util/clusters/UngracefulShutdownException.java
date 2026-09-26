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
package org.apache.kafka.connect.util.clusters;

import org.apache.kafka.common.KafkaException;

/**
 * An exception that can be used from within an {@code Exit.Procedure} to mask exit or halt calls
 * and signify that the service terminated abruptly. It's intended to be used only from within
 * integration tests.
 */
public class UngracefulShutdownException extends KafkaException {
    public UngracefulShutdownException(String s) {
        super(s);
    }

    public UngracefulShutdownException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public UngracefulShutdownException(Throwable throwable) {
        super(throwable);
    }
}
