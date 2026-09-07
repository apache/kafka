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

// Aiven fork addition (KAFKA-20295), name kept verbatim from apache/kafka#22191 (KIP-1312): it
// describes the condition (no such registration), not the operation, so it is shared unmodified
// between decommission-controller (this fork) and unregister-controller (upstream 4.4+).
public class ControllerIdNotRegisteredException extends ApiException {

    public ControllerIdNotRegisteredException(String message) {
        super(message);
    }

    public ControllerIdNotRegisteredException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
