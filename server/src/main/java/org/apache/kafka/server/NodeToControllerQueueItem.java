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

package org.apache.kafka.server;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;

/**
 * Represents a queued request to be sent to the controller.
 * Used for timeout tracking and asynchronous completion handling.
 *
 * @param createdTimeMs timestamp when this request was created, used for timeout detection
 * @param request the request to send to the controller
 * @param callback handler invoked when the request completes, fails, or times out
 */
public record NodeToControllerQueueItem(Long createdTimeMs,
                                        AbstractRequest.Builder<? extends AbstractRequest> request,
                                        ControllerRequestCompletionHandler callback) {
}
