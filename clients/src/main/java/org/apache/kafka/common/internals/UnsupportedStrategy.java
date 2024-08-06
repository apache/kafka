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
package org.apache.kafka.common.internals;

import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/**
 * This is a fallback strategy to use if no other strategies are available.
 * <p>This is used to improve control flow and provide detailed error messages in unusual situations.
 */
class UnsupportedStrategy implements SecurityManagerCompatibility {

    private final Throwable e1;
    private final Throwable e2;

    UnsupportedStrategy(Throwable e1, Throwable e2) {
        this.e1 = e1;
        this.e2 = e2;
    }

    private UnsupportedOperationException createException(String message) {
        UnsupportedOperationException e = new UnsupportedOperationException(message);
        e.addSuppressed(e1);
        e.addSuppressed(e2);
        return e;
    }

    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        throw createException("Unable to find suitable AccessController#doPrivileged implementation");
    }

    @Override
    public Subject current() {
        throw createException("Unable to find suitable Subject#getCurrent or Subject#current implementation");
    }

    @Override
    public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
        throw createException("Unable to find suitable Subject#doAs or Subject#callAs implementation");
    }
}
