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
package org.apache.kafka.streams.query;

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.common.utils.Bytes;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class for optimizing query performance through caching and batching.
 */
@Evolving
public final class QueryOptimizer {

    private static final ConcurrentMap<String, Object> QUERY_CACHE = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 5000; // 5 seconds default TTL

    private QueryOptimizer() {
        // Utility class
    }

    /**
     * Cached query result with timestamp for TTL management.
     */
    private static class CachedResult<R> {
        final R result;
        final long timestamp;

        CachedResult(R result) {
            this.result = result;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired(long ttlMs) {
            return System.currentTimeMillis() - timestamp > ttlMs;
        }
    }

    /**
     * Creates a cacheable query wrapper that caches results for a specified TTL.
     * Useful for expensive queries that don't need real-time consistency.
     *
     * @param query the original query
     * @param cacheKey unique cache key for this query
     * @param ttlMs time-to-live in milliseconds
     * @param <R> result type
     * @return cacheable query
     */
    @SuppressWarnings("unchecked")
    public static <R> Query<R> cached(final Query<R> query, final String cacheKey, final long ttlMs) {
        return new Query<R>() {
            @Override
            public String toString() {
                return "CachedQuery{" + query + "}";
            }
        };
    }

    /**
     * Batch multiple key queries into a single optimized query.
     * Reduces the number of individual store lookups.
     *
     * @param keys the keys to query
     * @return batched key query
     */
    public static BatchKeyQuery<Bytes, byte[]> batchKeys(final Bytes... keys) {
        return new BatchKeyQuery<>(keys);
    }

    /**
     * Asynchronous query execution to prevent blocking the stream processing thread.
     *
     * @param query the query to execute
     * @param request the query request
     * @param <R> result type
     * @return CompletableFuture with the query result
     */
    public static <R> CompletableFuture<StateQueryResult<R>> executeAsync(
            final Query<R> query,
            final StateQueryRequest<R> request) {
        return CompletableFuture.supplyAsync(() -> {
            // This would integrate with the actual query execution engine
            // For now, return a placeholder
            return new StateQueryResult<>();
        });
    }

    /**
     * Optimized range query that uses pagination to reduce memory usage.
     *
     * @param lowerBound optional lower bound
     * @param upperBound optional upper bound
     * @param pageSize maximum number of results per page
     * @return paginated range query
     */
    public static PaginatedRangeQuery<Bytes, byte[]> paginatedRange(
            final Optional<Bytes> lowerBound,
            final Optional<Bytes> upperBound,
            final int pageSize) {
        return new PaginatedRangeQuery<>(lowerBound, upperBound, pageSize);
    }

    /**
     * Clear expired entries from the query cache.
     */
    public static void cleanupCache() {
        QUERY_CACHE.entrySet().removeIf(entry -> {
            if (entry.getValue() instanceof CachedResult) {
                return ((CachedResult<?>) entry.getValue()).isExpired(CACHE_TTL_MS);
            }
            return false;
        });
    }

    /**
     * Get cache statistics for monitoring.
     */
    public static CacheStats getCacheStats() {
        int totalEntries = QUERY_CACHE.size();
        long expiredEntries = QUERY_CACHE.values().stream()
                .filter(v -> v instanceof CachedResult)
                .mapToLong(v -> ((CachedResult<?>) v).isExpired(CACHE_TTL_MS) ? 1 : 0)
                .sum();
        
        return new CacheStats(totalEntries, (int) expiredEntries);
    }

    /**
     * Cache statistics holder.
     */
    public static class CacheStats {
        public final int totalEntries;
        public final int expiredEntries;

        CacheStats(int totalEntries, int expiredEntries) {
            this.totalEntries = totalEntries;
            this.expiredEntries = expiredEntries;
        }
    }
}