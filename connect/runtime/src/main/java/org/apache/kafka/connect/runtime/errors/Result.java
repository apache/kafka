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
package org.apache.kafka.connect.runtime.errors;

/**
 * The result of evaluating an {@link Operation}.
 * @param <V> return type of the result of the operation.
 */
public final class Result<V> {

    private final V result;
    private final Throwable error;

    public Result(V result) {
        this(result, null);
    }

    public Result(Throwable error) {
        this(null, error);
    }

    private Result(V result, Throwable error) {
        this.result = result;
        this.error = error;
    }

    /**
     * @return the result of the operation.
     */
    public V result() {
        return result;
    }

    /**
     * @return if true, the operation evaluated successfully; false otherwise
     */
    public boolean success() {
        return error == null;
    }

    /**
     * @return errors if the operation did not succeed.
     */
    public Throwable error() {
        return error;
    }
}
