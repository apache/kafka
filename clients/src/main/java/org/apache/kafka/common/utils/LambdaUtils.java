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
package org.apache.kafka.common.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lambda helpers.
 */
@FunctionalInterface
public interface LambdaUtils {
    /**
     * Run some code, possibly throw some exceptions.
     *
     * @throws Exception
     */
    void run() throws Exception;

    /**
     * Provide an idempotent instance of the supplied code - ensure that the supplied code gets run only once, no
     * matter how many times .run() is called.
     */
    static Runnable idempotent(final Runnable code) {
        return new Runnable() {
            boolean run = false;

            public void run() {
                if (run)
                    return;

                run = true;
                code.run();
            }
        };
    }

    /**
     * Run the supplied code. If an exception is thrown, it is swallowed and registered to the firstException parameter.
     */
    static void swallow(final LambdaUtils code, final AtomicReference<Throwable> firstException) {
        if (code != null) {
            try {
                code.run();
            } catch (Exception t) {
                firstException.compareAndSet(null, t);
            }
        }
    }

    static RuntimeException wrap(final Exception ex) {
        return ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
    }

    /**
     * Run the supplied callable, wrapping non-runtime exceptions in runtime exceptions.
     */
    static <T> T wrapThrow(final Callable<T> code) {
        try {
            return code.call();
        } catch (Exception ex) {
            throw wrap(ex);
        }
    }
}
