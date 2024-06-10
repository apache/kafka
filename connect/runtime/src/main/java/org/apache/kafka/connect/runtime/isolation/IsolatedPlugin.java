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
        this.classLoader = plugins.pluginLoader(delegate);
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

    protected void isolate(ThrowingRunnable runnable) throws Exception {
        isolate(() -> {
            runnable.run();
            return null;
        });
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            plugins,
            pluginClass,
            delegate,
            classLoader,
            type
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        IsolatedPlugin<?> other = (IsolatedPlugin<?>) obj;
        return
            this.plugins == other.plugins
                && this.pluginClass == other.pluginClass
                // use reference equality, as plugin implementations may mis-implement equals
                && this.delegate == other.delegate
                && this.classLoader == other.classLoader
                && this.type == other.type;
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
