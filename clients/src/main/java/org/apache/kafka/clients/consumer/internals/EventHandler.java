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

package org.apache.kafka.clients.consumer.internals;

/**
 * The EventHandler interfaces the client and handler implementation.  The client should add an event for consumption,
 * and try to poll for the responses.  Here, one could poll a response with type K, and add a request with type T.
 * @param <T> Event request type
 * @param <K> Event response type
 */
public interface EventHandler<T, K> {
    public K poll();
    public boolean add(T event);
}
