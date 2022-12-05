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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class CompletableFutureUtils {

    /**
     * Returns a new {@link CompletableFuture} which completes to a list of all values of its input
     * completable futures if all succeed. Otherwise, the returned completable future is completed
     * exceptionally with a @{link CompletionException} holding the original exception.
     */
    public static <T> CompletableFuture<List<T>> allAsList(
        List<CompletableFuture<T>> futures
    ) {
        @SuppressWarnings("unchecked")
        CompletableFuture<T>[] all = new CompletableFuture[futures.size()];
        for (int i = 0; i < futures.size(); i++) {
            all[i] = futures.get(i);
        }

        return CompletableFuture.allOf(all).thenApply(__ -> {
            List<T> results = new ArrayList<>(all.length);
            for (CompletableFuture<T> future : all) {
                results.add(future.join());
            }
            return results;
        });
    }

}
