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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Utility methods for strategies which use reflection to access methods without requiring them at compile-time.
 */
class ReflectiveStrategy {

    static Object invoke(Method method, Object obj, Object... args) {
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException(e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    static <T extends Exception> Object invokeChecked(Method method, Class<T> ex, Object obj, Object... args) throws T {
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException(e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (ex.isInstance(cause)) {
                throw ex.cast(cause);
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    /**
     * Interface to allow mocking out classloading infrastructure. This is used to test reflective operations.
     */
    interface Loader {
        Class<?> loadClass(String className) throws ClassNotFoundException;

        static Loader forName() {
            return className -> Class.forName(className, true, Loader.class.getClassLoader());
        }
    }
}
