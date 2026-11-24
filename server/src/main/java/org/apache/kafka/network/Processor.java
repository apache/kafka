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
package org.apache.kafka.network;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.server.ApiVersionManager;

import java.nio.ByteBuffer;

public class Processor {

    public static final String IDLE_PERCENT_METRIC_NAME = "IdlePercent";
    public static final String NETWORK_PROCESSOR_METRIC_TAG = "networkProcessor";
    public static final String LISTENER_METRIC_TAG = "listener";
    public static final int CONNECTION_QUEUE_SIZE = 20;

    public static RequestHeader parseRequestHeader(ApiVersionManager apiVersionManager, ByteBuffer buffer) {
        RequestHeader header = RequestHeader.parse(buffer);
        if (apiVersionManager.isApiEnabled(header.apiKey(), header.apiVersion())) {
            return header;
        } else if (header.isApiVersionSupported()) {
            throw new InvalidRequestException("Received request for disabled api with key " + header.apiKey().id + " (" + header.apiKey().name() + ") and version " + header.apiVersion());
        } else {
            throw new UnsupportedVersionException("Received request for api with key " + header.apiKey().id + " (" + header.apiKey().name + ") and unsupported version " + header.apiVersion());
        }
    }
}
