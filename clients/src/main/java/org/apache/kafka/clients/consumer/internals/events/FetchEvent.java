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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.CompletedFetch;

import java.util.Queue;

/**
 * This event signals the background thread to submit a fetch request.
 */
public class FetchEvent<K, V> extends CompletableApplicationEvent<Queue<CompletedFetch<K, V>>> {

    public FetchEvent() {
        super(Type.FETCH);
    }

    @Override
    public String toString() {
        return "FetchEvent{" +
                "future=" + future +
                ", type=" + type +
                '}';
    }
}
