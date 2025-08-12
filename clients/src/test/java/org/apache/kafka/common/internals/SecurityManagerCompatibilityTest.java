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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import javax.security.auth.Subject;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SecurityManagerCompatibilityTest {

    @EnabledForJreRange(max = JRE.JAVA_22)
    @Test
    public void testLegacyStrategyLoadable() throws ClassNotFoundException, NoSuchMethodException {
        new LegacyStrategy(ReflectiveStrategy.Loader.forName());
    }

    @EnabledForJreRange(min = JRE.JAVA_18)
    @Test
    public void testModernStrategyLoadable() throws ClassNotFoundException, NoSuchMethodException {
        new ModernStrategy(ReflectiveStrategy.Loader.forName());
    }

    @Test
    public void testCompositeStrategyLoadable() {
        new CompositeStrategy(ReflectiveStrategy.Loader.forName());
    }

    @Test
    public void testDefaultStrategyLoadable() {
        assertNotNull(SecurityManagerCompatibility.get());
    }

    @Test
    public void testDefaultStrategyDoPrivilegedReturn() {
        Object object = new Object();
        Object returned = SecurityManagerCompatibility.get().doPrivileged(() -> object);
        assertSame(object, returned);
    }

    @Test
    public void testDefaultStrategyDoPrivilegedThrow() {
        assertThrows(RuntimeException.class, () ->
                SecurityManagerCompatibility.get().doPrivileged(() -> {
                    throw new RuntimeException();
                })
        );
    }

    @Test
    public void testDefaultStrategyCurrentNull() {
        Subject current = SecurityManagerCompatibility.get().current();
        assertNull(current);
    }

    @Test
    public void testDefaultStrategyCallAsReturn() {
        Subject subject = new Subject();
        Object object = new Object();
        Object returned = SecurityManagerCompatibility.get().callAs(subject, () -> object);
        assertSame(object, returned);
    }

    @Test
    public void testDefaultStrategyCallAsCurrent() {
        Subject subject = new Subject();
        Subject returned = SecurityManagerCompatibility.get().callAs(subject, SecurityManagerCompatibility.get()::current);
        assertSame(subject, returned);
    }

    @Test
    public void testLegacyStrategyThrowsWhenSecurityManagerRemoved() {
        ReflectiveStrategy.Loader loader = simulateSecurityManagerRemoval();
        assertThrows(ClassNotFoundException.class, () -> new LegacyStrategy(loader));
    }

    @EnabledForJreRange(min = JRE.JAVA_18)
    @Test
    public void testModernStrategyLoadableWhenSecurityManagerRemoved() throws ClassNotFoundException, NoSuchMethodException {
        ReflectiveStrategy.Loader loader = simulateSecurityManagerRemoval();
        new ModernStrategy(loader);
    }

    @Test
    public void testCompositeStrategyLoadableWhenSecurityManagerRemoved() {
        ReflectiveStrategy.Loader loader = simulateSecurityManagerRemoval();
        new CompositeStrategy(loader);
    }

    @Test
    public void testLegacyStrategyCurrentThrowsWhenSecurityManagerUnsupported() throws ClassNotFoundException, NoSuchMethodException {
        ReflectiveStrategy.Loader loader = simulateMethodsThrowUnsupportedOperationExceptions();
        SecurityManagerCompatibility legacy = new LegacyStrategy(loader);
        assertThrows(UnsupportedOperationException.class, legacy::current);
    }

    @Test
    public void testLegacyStrategyCallAsThrowsWhenSecurityManagerUnsupported() throws ClassNotFoundException, NoSuchMethodException {
        ReflectiveStrategy.Loader loader = simulateMethodsThrowUnsupportedOperationExceptions();
        SecurityManagerCompatibility legacy = new LegacyStrategy(loader);
        assertThrows(UnsupportedOperationException.class, () -> legacy.callAs(null, () -> null));
    }

    @Test
    public void testCompositeStrategyDoPrivilegedWhenSecurityManagerUnsupported() {
        ReflectiveStrategy.Loader loader = simulateMethodsThrowUnsupportedOperationExceptions();
        CompositeStrategy composite = new CompositeStrategy(loader);
        Object object = new Object();
        Object returned = composite.doPrivileged(() -> object);
        assertSame(object, returned);
    }

    @Test
    public void testCompositeStrategyCurrentWhenSecurityManagerUnsupported() {
        ReflectiveStrategy.Loader loader = simulateMethodsThrowUnsupportedOperationExceptions();
        CompositeStrategy composite = new CompositeStrategy(loader);
        Object returned = composite.current();
        assertNull(returned);
    }

    @Test
    public void testCompositeStrategyCallAsWhenSecurityManagerUnsupported() {
        ReflectiveStrategy.Loader loader = simulateMethodsThrowUnsupportedOperationExceptions();
        CompositeStrategy composite = new CompositeStrategy(loader);
        Subject subject = new Subject();
        Subject returned = composite.callAs(subject, composite::current);
        assertSame(subject, returned);
    }

    private ReflectiveStrategy.Loader simulateSecurityManagerRemoval() {
        return name -> {
            if (name.equals("java.security.AccessController")) {
                throw new ClassNotFoundException();
            } else {
                return ReflectiveStrategy.Loader.forName().loadClass(name);
            }
        };
    }

    private ReflectiveStrategy.Loader simulateMethodsThrowUnsupportedOperationExceptions() {
        // WARNING: These assertions are here to prevent warnings about unused methods.
        // These methods are used reflectively, and can't be removed.
        assertThrows(UnsupportedOperationException.class, () -> UnsupportedOperations.doPrivileged(null));
        assertThrows(UnsupportedOperationException.class, () -> UnsupportedOperations.getSubject(null));
        assertThrows(UnsupportedOperationException.class, () -> UnsupportedOperations.doAs(null, null));
        assertNull(UnsupportedOperations.current());
        assertNull(UnsupportedOperations.callAs(null, () -> null));
        return name -> {
            switch (name) {
                case "java.security.AccessController":
                case "javax.security.auth.Subject":
                    return UnsupportedOperations.class;
                case "java.security.AccessControlContext":
                    return UnsupportedOperations.DummyContext.class;
                default:
                    return ReflectiveStrategy.Loader.forName().loadClass(name);
            }
        };
    }

    /**
     * This is a class meant to stand-in for the AccessController, and Subject classes.
     * It simulates a scenario where all legacy methods throw UnsupportedOperationException, and only the modern
     * methods are functional.
     */
    public static class UnsupportedOperations {

        /**
         * This class stands-in for the AccessControlContext in the mocked signatures below, because we can't have a
         * compile-time dependency on the real class. This needs no methods and is just a dummy class.
         */
        public static class DummyContext {
        }

        private static final ThreadLocal<Subject> ACTIVE_SUBJECT = new ThreadLocal<>();

        /**
         * Copy of AccessController#doPrivileged
         */
        public static <T> void doPrivileged(PrivilegedAction<T> ignored) {
            throw new UnsupportedOperationException();
        }

        /**
         * Copy of AccessController#getContext
         */
        public static DummyContext getContext() {
            throw new UnsupportedOperationException();
        }

        /**
         * Copy of Subject#getSubject
         */
        public static void getSubject(DummyContext ignored) {
            throw new UnsupportedOperationException();
        }

        /**
         * Copy of Subject#doAs
         */
        public static <T> void doAs(Subject ignored1, PrivilegedExceptionAction<T> ignored2) {
            throw new UnsupportedOperationException();
        }

        /**
         * Copy of Subject#current
         */
        public static Subject current() {
            return ACTIVE_SUBJECT.get();
        }

        /**
         * Copy of Subject#callAs
         */
        public static <T> T callAs(Subject subject, Callable<T> action) throws CompletionException {
            Subject previous = ACTIVE_SUBJECT.get();
            ACTIVE_SUBJECT.set(subject);
            try {
                return action.call();
            } catch (Throwable e) {
                throw new CompletionException(e);
            } finally {
                ACTIVE_SUBJECT.set(previous);
            }
        }
    }
}
