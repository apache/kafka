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

import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.security.auth.Subject;

/**
 * This strategy combines the functionality of the {@link LegacyStrategy}, {@link ModernStrategy}, and
 * {@link UnsupportedStrategy} strategies to provide the legacy APIs as long as they are present and not degraded.
 * If the legacy APIs are missing or degraded, this falls back to the modern APIs.
 */
class CompositeStrategy implements SecurityManagerCompatibility {

    private static final Logger log = LoggerFactory.getLogger(CompositeStrategy.class);
    static final CompositeStrategy INSTANCE = new CompositeStrategy(ReflectiveStrategy.Loader.forName());

    private final SecurityManagerCompatibility fallbackStrategy;
    private final AtomicReference<SecurityManagerCompatibility> activeStrategy;

    // Visible for testing
    CompositeStrategy(ReflectiveStrategy.Loader loader) {
        SecurityManagerCompatibility initial;
        SecurityManagerCompatibility fallback = null;
        try {
            initial = new LegacyStrategy(loader);
            try {
                fallback = new ModernStrategy(loader);
                // This is expected for JRE 18+
                log.debug("Loaded legacy SecurityManager methods, will fall back to modern methods after UnsupportedOperationException");
            } catch (NoSuchMethodException | ClassNotFoundException ex) {
                // This is expected for JRE <= 17
                log.debug("Unable to load modern Subject methods, relying only on legacy methods", ex);
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            try {
                initial = new ModernStrategy(loader);
                // This is expected for JREs after the removal takes place.
                log.debug("Unable to load legacy SecurityManager methods, relying only on modern methods", e);
            } catch (NoSuchMethodException | ClassNotFoundException ex) {
                initial = new UnsupportedStrategy(e, ex);
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
