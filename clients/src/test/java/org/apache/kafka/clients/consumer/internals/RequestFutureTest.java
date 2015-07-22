/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequestFutureTest {

    @Test
    public void testComposeSuccessCase() {
        RequestFuture<String> future = new RequestFuture<String>();
        RequestFuture<Integer> composed = future.compose(new RequestFutureAdapter<String, Integer>() {
            @Override
            public void onSuccess(String value, RequestFuture<Integer> future) {
                future.complete(value.length());
            }
        });

        future.complete("hello");

        assertTrue(composed.isDone());
        assertTrue(composed.succeeded());
        assertEquals(5, (int) composed.value());
    }

    @Test
    public void testComposeFailureCase() {
        RequestFuture<String> future = new RequestFuture<String>();
        RequestFuture<Integer> composed = future.compose(new RequestFutureAdapter<String, Integer>() {
            @Override
            public void onSuccess(String value, RequestFuture<Integer> future) {
                future.complete(value.length());
            }
        });

        RuntimeException e = new RuntimeException();
        future.raise(e);

        assertTrue(composed.isDone());
        assertTrue(composed.failed());
        assertEquals(e, composed.exception());
    }

}
