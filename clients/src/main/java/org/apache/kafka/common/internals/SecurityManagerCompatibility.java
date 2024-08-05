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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.security.auth.Subject;

/**
 * This is a compatibility class to provide dual-support for JREs with and without SecurityManager support.
 * <p>Users should call {@link #get()} to retrieve a singleton instance, and call instance methods
 * {@link #doPrivileged(PrivilegedAction)}, {@link #current()}, and {@link #callAs(Subject, Callable)}.
 * <p>This class's motivation and expected behavior is defined in
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1006%3A+Remove+SecurityManager+Support">KIP-1006</a>
 */
public interface SecurityManagerCompatibility {

    static SecurityManagerCompatibility get() {
        return Composite.INSTANCE;
    }

    <T> T doPrivileged(PrivilegedAction<T> action);

    Subject current();

    <T> T callAs(Subject subject, Callable<T> action) throws CompletionException;

    interface Loader {
        Class<?> loadClass(String className) throws ClassNotFoundException;

        static Loader forName() {
            return className -> Class.forName(className, true, Loader.class.getClassLoader());
        }
    }

    /**
     * This class implements reflective access to the deprecated-for-removal methods of AccessController and Subject.
     * <p>Instantiating this class may fail if any of the required classes or methods are not found.
     * Method invocations for this class may fail with {@link UnsupportedOperationException} if all methods are found,
     * but the operation is not permitted to be invoked.
     * <p>This class is expected to be instantiable in JRE >=8 until the removal finally takes place.
     */
    @SuppressWarnings("unchecked")
    class Legacy implements SecurityManagerCompatibility {

        private final Method doPrivileged;
        private final Method getContext;
        private final Method getSubject;
        private final Method doAs;

        // Visible for testing
        Legacy(Loader loader) throws ClassNotFoundException, NoSuchMethodException {
            Class<?> accessController = loader.loadClass("java.security.AccessController");
            doPrivileged = accessController.getDeclaredMethod("doPrivileged", PrivilegedAction.class);
            getContext = accessController.getDeclaredMethod("getContext");
            Class<?> accessControlContext = loader.loadClass("java.security.AccessControlContext");
            Class<?> subject = loader.loadClass(Subject.class.getName());
            getSubject = subject.getDeclaredMethod("getSubject", accessControlContext);
            // Note that we use the real Subject.class rather than the `subject` variable
            // This allows for mocking out the method without changing the argument types.
            doAs = subject.getDeclaredMethod("doAs", Subject.class, PrivilegedExceptionAction.class);
        }

        @Override
        public <T> T doPrivileged(PrivilegedAction<T> action) {
            return (T) invoke(doPrivileged, null, action);
        }

        /**
         * @return the result of AccessController.getContext(), of type AccessControlContext
         */
        private Object getContext() {
            return invoke(getContext, null);
        }

        /**
         * @param context The current AccessControlContext
         * @return The result of Subject.getSubject(AccessControlContext)
         */
        private Subject getSubject(Object context) {
            return (Subject) invoke(getSubject,null, context);
        }

        @Override
        public Subject current() {
            return getSubject(getContext());
        }

        /**
         * @return The result of Subject.doAs(Subject, PrivilegedExceptionAction)
         */
        private <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
            return (T) invoke(doAs, null, subject, action);
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

    /**
     * This class implements reflective access to the methods of Subject added to replace deprecated methods.
     * <p>Instantiating this class may fail if any of the required classes or methods are not found.
     * Method invocations for this class may fail with {@link UnsupportedOperationException} if all methods are found,
     * but the operation is not permitted to be invoked.
     * <p>This class is expected to be instantiable in JRE >= 18. At the time of writing, these methods do not have
     * a sunset date, and are expected to be available past the removal of the SecurityManager.
     */
    @SuppressWarnings("unchecked")
    class Modern implements SecurityManagerCompatibility {

        private final Method current;
        private final Method callAs;

        // Visible for testing
        Modern(Loader loader) throws NoSuchMethodException, ClassNotFoundException {
            Class<?> subject = loader.loadClass(Subject.class.getName());
            current = subject.getDeclaredMethod("current");
            // Note that we use the real Subject.class rather than the `subject` variable
            // This allows for mocking out the method without changing the argument types.
            callAs = subject.getDeclaredMethod("callAs", Subject.class, Callable.class);
        }

        @Override
        public <T> T doPrivileged(PrivilegedAction<T> action) {
            // This is intentionally a pass-through
            return action.run();
        }

        @Override
        public Subject current() {
            return (Subject) invoke(current, null);
        }

        @Override
        public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
            return (T) invoke(callAs, null, subject, action);
        }
    }

    /**
     * This is the fallback to use if both {@link Legacy} and {@link Modern} compatibility strategies are unsuitable.
     * <p>This is used to improve control flow and provide detailed error messages in unusual situations.
     */
    class Unsupported implements SecurityManagerCompatibility {

        private final Throwable e1;
        private final Throwable e2;

        private Unsupported(Throwable e1, Throwable e2) {
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

    /**
     * This strategy combines the functionality of the {@link Legacy}, {@link Modern}, and {@link Unsupported}
     * strategies to provide the legacy APIs as long as they are present and not degraded. If the legacy APIs are
     * missing or degraded, this falls back to the modern APIs.
     */
    class Composite implements SecurityManagerCompatibility {

        private static final Logger log = LoggerFactory.getLogger(Composite.class);
        private static final Composite INSTANCE = new Composite(Loader.forName());

        private final SecurityManagerCompatibility fallbackStrategy;
        private final AtomicReference<SecurityManagerCompatibility> activeStrategy;

        // Visible for testing
        Composite(Loader loader) {
            SecurityManagerCompatibility initial;
            SecurityManagerCompatibility fallback = null;
            try {
                initial = new Legacy(loader);
                try {
                    fallback = new Modern(loader);
                    // This is expected for JRE 18+
                    log.debug("Loaded legacy SecurityManager methods, will fall back to modern methods after UnsupportedOperationException");
                } catch (NoSuchMethodException | ClassNotFoundException ex) {
                    // This is expected for JRE <= 17
                    log.debug("Unable to load modern Subject methods, relying only on legacy methods", ex);
                }
            } catch (ClassNotFoundException | NoSuchMethodException e) {
                try {
                    initial = new Modern(loader);
                    // This is expected for JREs after the removal takes place.
                    log.debug("Unable to load legacy SecurityManager methods, relying only on modern methods", e);
                } catch (NoSuchMethodException | ClassNotFoundException ex) {
                    initial = new Unsupported(e, ex);
                    // This is not expected in normal use, only in test environments.
                    log.error("Unable to load legacy SecurityManager methods", e);
                    log.error("Unable to load modern Subject methods", ex);
                }
            }
            Objects.requireNonNull(initial, "initial strategy must be defined");
            activeStrategy = new AtomicReference<>(initial);
            fallbackStrategy = fallback;
        }

        private <T> T performAction(Function<SecurityManagerCompatibility, T> action) {
            SecurityManagerCompatibility active = activeStrategy.get();
            try {
                return action.apply(active);
            } catch (UnsupportedOperationException e) {
                // If we chose a fallback strategy during loading, switch to it and retry this operation.
                if (active != fallbackStrategy && fallbackStrategy != null) {
                    if (activeStrategy.compareAndSet(active, fallbackStrategy)) {
                        log.debug("Using fallback strategy after encountering degraded legacy method", e);
                    }
                    return action.apply(fallbackStrategy);
                }
                // If we're already using the fallback strategy, then there's nothing to do to handle these exceptions.
                throw e;
            }
        }

        @Override
        public <T> T doPrivileged(PrivilegedAction<T> action) {
            return performAction(compatibility -> compatibility.doPrivileged(action));
        }

        @Override
        public Subject current() {
            return performAction(SecurityManagerCompatibility::current);
        }

        @Override
        public <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
            return performAction(compatibility -> compatibility.callAs(subject, action));
        }
    }

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
}
