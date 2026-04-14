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
package org.apache.kafka.network;

import org.apache.kafka.server.common.RequestLocal;

import java.util.Objects;
import java.util.function.Consumer;

public final class CallbackRequest implements BaseRequest {
    private final Consumer<RequestLocal> fun;
    private final Request originalRequest;

    public CallbackRequest(Consumer<RequestLocal> fun, Request originalRequest) {
        this.fun = fun;
        this.originalRequest = originalRequest;
    }

    public Consumer<RequestLocal> fun() {
        return fun;
    }

    public Request originalRequest() {
        return originalRequest;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CallbackRequest that)) return false;
        return Objects.equals(fun, that.fun) && Objects.equals(originalRequest, that.originalRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fun, originalRequest);
    }

    @Override
    public String toString() {
        return "CallbackRequest(" + fun + ", " + originalRequest + ")";
    }
}
