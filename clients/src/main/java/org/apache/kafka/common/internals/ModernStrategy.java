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

import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/**
 * This class implements reflective access to the methods of Subject added to replace deprecated methods.
 * <p>Instantiating this class may fail if any of the required classes or methods are not found.
 * Method invocations for this class may fail with {@link UnsupportedOperationException} if all methods are found,
 * but the operation is not permitted to be invoked.
 * <p>This class is expected to be instantiable in JRE >= 18. At the time of writing, these methods do not have
 * a sunset date, and are expected to be available past the removal of the SecurityManager.
 */
@SuppressWarnings("unchecked")
class ModernStrategy implements SecurityManagerCompatibility {

    private final Method current;
    private final Method callAs;

    // Visible for testing
    ModernStrategy(ReflectiveStrategy.Loader loader) throws NoSuchMethodException, ClassNotFoundException {
        Class<?> subject = loader.loadClass(Subject.class.getName());
        current = subject.getDeclaredMethod("current");
        // Note that the Subject class isn't deprecated or removed, so reference it as an argument type.
        // This allows for mocking out the method implementation while still accepting Subject instances as arguments.
        callAs = subject.getDeclaredMethod("callAs", Subject.class, Callable.class);
    }

    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        // This is intentionally a pass-through
        return action.run();
    }

    @Override
    public Subject current() {
        return (Subject) ReflectiveStrategy.invoke(current, null);
    }

    @Override
    public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
        return (T) ReflectiveStrategy.invokeChecked(callAs, CompletionException.class, null, subject, action);
    }
}
