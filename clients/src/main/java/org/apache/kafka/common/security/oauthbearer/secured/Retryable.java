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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.util.concurrent.ExecutionException;

/**
 * Simple interface to abstract out the call that is made so that it can be retried.
 *
 * @param <R> Result type
 *
 * @see Retry
 * @see UnretryableException
 */

public interface Retryable<R> {

    /**
     * Perform the operation and return the data from the response.
     *
     * @return Return response data, formatted in the given data type
     *
     * @throws ExecutionException   Thrown on errors connecting, writing, reading, timeouts, etc.
     *                              that can likely be tried again
     * @throws UnretryableException Thrown on errors that we can determine should not be tried again
     */

    R call() throws ExecutionException, UnretryableException;

}
