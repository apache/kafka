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
package org.apache.kafka.connect.runtime.isolation;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class IsolatedPlugin<P> {

    private final Plugins plugins;
    private final Class<?> pluginClass;
    protected final P delegate;
    private final ClassLoader classLoader;
    private final PluginType type;

    IsolatedPlugin(Plugins plugins, P delegate, PluginType type) {
        this.plugins = Objects.requireNonNull(plugins, "plugins must be non-null");
        this.delegate = Objects.requireNonNull(delegate, "delegate plugin must be non-null");
        this.pluginClass = delegate.getClass();
        ClassLoader classLoader = pluginClass.getClassLoader();
        this.classLoader = Objects.requireNonNull(classLoader, "delegate plugin must not be a boostrap class");
        this.type = Objects.requireNonNull(type, "plugin type must be non-null");
    }

    public PluginType type() {
        return type;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends P> pluginClass() {
        return (Class<? extends P>) pluginClass;
    }

    protected <V> V isolate(Callable<V> callable) throws Exception {
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            return callable.call();
        }
    }

    protected void isolateV(ThrowingRunnable runnable) throws Exception {
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            runnable.run();
        }
    }

    protected <T> void isolateV(Consumer<T> consumer, T t) throws Exception {
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            consumer.accept(t);
        }
    }

    protected <T, U> void isolateV(BiConsumer<T, U> consumer, T t, U u) throws Exception {
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            consumer.accept(t, u);
        }
    }

    protected <T, R> R isolate(Function<T, R> function, T t) throws Exception {
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            return function.apply(t);
        }
    }

    protected <T, U, R> R isolate(BiFunction<T, U, R> function, T t, U u) throws Exception {
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            return function.apply(t, u);
        }
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Override
    public int hashCode() {
        try {
            return isolate(delegate::hashCode);
        } catch (Throwable e) {
            throw new RuntimeException("unable to evaluate plugin hashCode", e);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        IsolatedPlugin<?> other = (IsolatedPlugin<?>) obj;
        try {
            return isolate(delegate::equals, other.delegate);
        } catch (Throwable e) {
            throw new RuntimeException("unable to evaluate plugin equals", e);
        }
    }

    @Override
    public String toString() {
        try {
            return isolate(delegate::toString);
        } catch (Throwable e) {
            return "unable to evaluate plugin toString: " + e.getMessage();
        }
    }
}
