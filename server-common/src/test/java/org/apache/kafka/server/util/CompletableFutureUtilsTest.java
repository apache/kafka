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
package org.apache.kafka.server.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompletableFutureUtilsTest {

    @Test
    public void testAllAsList() throws ExecutionException, InterruptedException {
        CompletableFuture<Integer> future1 = new CompletableFuture<>();
        CompletableFuture<Integer> future2 = new CompletableFuture<>();
        CompletableFuture<Integer> future3 = new CompletableFuture<>();

        CompletableFuture<List<Integer>> result = CompletableFutureUtils.allAsList(Arrays.asList(
            future1,
            future2,
            future3
        ));

        assertFalse(result.isDone());

        future1.complete(1);
        future2.complete(2);
        future3.complete(3);

        assertTrue(result.isDone());
        assertEquals(Arrays.asList(1, 2, 3), result.get());
    }

    @Test
    public void testAllAsListCompletesExceptionally() throws ExecutionException, InterruptedException {
        CompletableFuture<Integer> future1 = new CompletableFuture<>();
        CompletableFuture<Integer> future2 = new CompletableFuture<>();
        CompletableFuture<Integer> future3 = new CompletableFuture<>();

        CompletableFuture<List<Integer>> result = CompletableFutureUtils.allAsList(Arrays.asList(
            future1,
            future2,
            future3
        ));

        assertFalse(result.isDone());

        future1.complete(1);
        future2.complete(2);
        future3.completeExceptionally(new IllegalStateException("exception"));

        assertTrue(result.isDone());
        assertTrue(result.isCompletedExceptionally());

        // `get` re-wraps the exception in ExecutionException.
        ExecutionException exception = assertThrows(ExecutionException.class, result::get);
        assertTrue(exception.getCause() instanceof IllegalStateException);

        // `allAsList` wraps the exception in CompletionException.
        CompletableFuture<Boolean> checkException = result.handle((__, e) ->
            e instanceof CompletionException && e.getCause() instanceof IllegalStateException
        );
        assertTrue(checkException.get());
    }
}
