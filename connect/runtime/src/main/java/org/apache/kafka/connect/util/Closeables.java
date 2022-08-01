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
package org.apache.kafka.connect.util;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class allows developers to easily track multiple {@link AutoCloseable} objects allocated
 * at different times and, if necessary {@link AutoCloseable#close} them.
 * <p>
 * Users should create an instance in a try-with-resources block,
 * {@link #register(AutoCloseable, String) register} resources that they allocate inside that
 * block, and optionally {@link #clear() clear} those resources before exiting the block if they
 * will be used outside it.
 * <p>
 * For example:
 * <pre> {@code
 * try (Closeables closeables = new Closeables()) {
 *      // Switch to the connector's classloader if something goes wrong and we have to close the
 *      // resources we've allocated for it
 *      closeables.useLoader(connectorClassLoader);
 *
 *     Converter keyConverter = createConverter(KEY_CONVERTER_CONFIG, connectorConfig);
 *     // Close the key converter if any of the next steps fails
 *     closeables.register(keyConverter, "task key converter");
 *
 *     Converter valueConverter = createConverter(VALUE_CONVERTER_CONFIG, connectorConfig);
 *     // Close the value converter if any of the next steps fails
 *     closeables.register(valueConverter, "task value converter);
 *
 *     HeaderConverter headerConverter = createHeaderConverter(connectorConfig);
 *     // Close the header converter if any of the next steps fails
 *     closeables.register(headerConverter, "task header converter);
 *
 *     WorkerTask workerTask = createWorkerTask(keyConverter, valueConverter, headerConverter);
 *
 *     // We've successfully created our task; clear our closeables since we want to keep using
 *     // these
 *     closeables.clear();
 * }
 * }</pre>
 *
 * Thread safety: this class is not thread-safe and is only intended to be accessed from a single
 * thread per instance.
 */
public class Closeables implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Closeables.class);

    private final Map<AutoCloseable, String> closeables;
    private ClassLoader loader;

    public Closeables() {
        closeables = new IdentityHashMap<>();
    }

    /**
     * Register a resource to be {@link AutoCloseable#close() closed} when this {@link Closeables}
     * object is {@link #close() closed}.
     * @param closeable the closeable resource to track; if null, will be silently ignored
     * @param name a description of the closeable resource to use for logging; may not be null
     */
    public void register(AutoCloseable closeable, String name) {
        Objects.requireNonNull(name, "name may not be null");
        if (closeable == null) {
            log.trace("Ignoring null closeable: {}", name);
        } else {
            log.trace("Registering closeable {}: {}", name, closeable);
            this.closeables.put(closeable, name);
        }
    }

    /**
     * Set a {@link ClassLoader} to switch to when {@link AutoCloseable#close() closing}
     * resources that have been {@link #register(AutoCloseable, String) registered}.
     * <p>
     * May not be invoked more than once.
     * @param loader the loader to use; may not be null
     * @throws IllegalStateException if invoked more than once
     */
    public void useLoader(ClassLoader loader) {
        if (this.loader != null) {
            throw new IllegalStateException("May only define classloader once");
        }
        Objects.requireNonNull(loader, "class loader may not be null");
        this.loader = loader;
    }

    /**
     * Forget any resources that have been {@link #register(AutoCloseable, String) registered}
     * before this call. Note that if a {@link ClassLoader} has been set via
     * {@link #useLoader(ClassLoader)}, it will remain set, and subsequent calls to
     * {@link #useLoader(ClassLoader)} will still fail.
     */
    public void clear() {
        closeables.clear();
    }

    /**
     * {@link AutoCloseable#close() Close} all resources that have been
     * {@link #register(AutoCloseable, String) registered}, using the {@link ClassLoader} set via
     * {@link #useLoader(ClassLoader)} (if one has been set), except those that have been forgotten
     * by a call to {@link #clear()}.
     * <p>
     * If any call to {@link AutoCloseable#close()} fails, the exception is caught, logged, and not
     * propagated to the caller.
     */
    @Override
    public void close() {
        if (closeables.isEmpty())
            return;

        try (Utils.UncheckedCloseable loaderSwap = maybeSwapLoaders()) {
            closeables.forEach(Utils::closeQuietly);
            closeables.clear();
        }
    }

    private Utils.UncheckedCloseable maybeSwapLoaders() {
        if (loader != null) {
            return new LoaderSwap(loader)::close;
        } else {
            return () -> { };
        }
    }

}
