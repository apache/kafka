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

import org.apache.kafka.common.annotation.InterfaceStability.Unstable;

import java.util.List;

/**
 * This utility class is an internal API, but it must be located in the same package as
 * {@link QueryResult} so that it can access the package-private constructor
 * {@link QueryResult#QueryResult(Object, List, Position)}
 */
@Unstable
public final class InternalQueryResultUtil {
    // uninstantiable utility class
    public InternalQueryResultUtil() {}

    public static <R> QueryResult<R> copyAndSubstituteDeserializedResult(
        final QueryResult<?> rawResult,
        final R deserializedResult) {

        if (rawResult.isFailure()) {
            throw new IllegalArgumentException(
                "Callers must avoid calling this method on a failed result."
            );
        } else {
            return new QueryResult<>(
                deserializedResult,
                rawResult.getExecutionInfo(),
                rawResult.getPosition()
            );
        }
    }
}
