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
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

/**
 * This class implements reflective access to the deprecated-for-removal methods of AccessController and Subject.
 * <p>Instantiating this class may fail if any of the required classes or methods are not found.
 * Method invocations for this class may fail with {@link UnsupportedOperationException} if all methods are found,
 * but the operation is not permitted to be invoked.
 * <p>This class is expected to be instantiable in JRE >=8 until the removal finally takes place.
 */
@SuppressWarnings("unchecked")
class LegacyStrategy implements SecurityManagerCompatibility {

    private final Method doPrivileged;
    private final Method getContext;
    private final Method getSubject;
    private final Method doAs;

    // Visible for testing
    LegacyStrategy(ReflectiveStrategy.Loader loader) throws ClassNotFoundException, NoSuchMethodException {
        Class<?> accessController = loader.loadClass("java.security.AccessController");
        doPrivileged = accessController.getDeclaredMethod("doPrivileged", PrivilegedAction.class);
        getContext = accessController.getDeclaredMethod("getContext");
        Class<?> accessControlContext = loader.loadClass("java.security.AccessControlContext");
        Class<?> subject = loader.loadClass(Subject.class.getName());
        getSubject = subject.getDeclaredMethod("getSubject", accessControlContext);
        // Note that the Subject class isn't deprecated or removed, so reference it as an argument type.
        // This allows for mocking out the method implementation while still accepting Subject instances as arguments.
        doAs = subject.getDeclaredMethod("doAs", Subject.class, PrivilegedExceptionAction.class);
    }

    @Override
    public <T> T doPrivileged(PrivilegedAction<T> action) {
        return (T) ReflectiveStrategy.invoke(doPrivileged, null, action);
    }

    /**
     * @return the result of AccessController.getContext(), of type AccessControlContext
     */
    private Object getContext() {
        return ReflectiveStrategy.invoke(getContext, null);
    }

    /**
     * @param context The current AccessControlContext
     * @return The result of Subject.getSubject(AccessControlContext)
     */
    private Subject getSubject(Object context) {
        return (Subject) ReflectiveStrategy.invoke(getSubject, null, context);
    }

    @Override
    public Subject current() {
        return getSubject(getContext());
    }

    /**
     * @return The result of Subject.doAs(Subject, PrivilegedExceptionAction)
     */
    private <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        return (T) ReflectiveStrategy.invokeChecked(doAs, PrivilegedActionException.class, null, subject, action);
    }

    @Override
    public <T> T callAs(Subject subject, Callable<T> callable) throws CompletionException {
        try {
            return doAs(subject, callable::call);
        } catch (PrivilegedActionException e) {
            throw new CompletionException(e.getCause());
        }
    }
}
