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
package org.apache.kafka.connect.runtime;


import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.util.Objects;
import java.util.function.Function;

/**
 * Wrapper for a {@link Transformation} and corresponding optional {@link Predicate}
 * which applies the transformation when the {@link Predicate} is true (or false, according to {@code negate}).
 * If no {@link Predicate} is provided, the transformation will be unconditionally applied.
 * @param <R> The type of record (must be an implementation of {@link ConnectRecord})
 */
public class TransformationStage<R extends ConnectRecord<R>> implements AutoCloseable {

    static final String PREDICATE_CONFIG = "predicate";
    static final String NEGATE_CONFIG = "negate";
    private final Plugin<Predicate<R>> predicatePlugin;
    private final Plugin<Transformation<R>> transformationPlugin;
    private final boolean negate;
    private final String transformAlias;
    private final String predicateAlias;
    private final String transformVersion;
    private final String predicateVersion;
    private final Function<ClassLoader, LoaderSwap> pluginLoaderSwapper;


    TransformationStage(
        Plugin<Transformation<R>> transformationPlugin,
        String transformAlias,
        String transformVersion,
        Function<ClassLoader, LoaderSwap> pluginLoaderSwapper
    ) {
        this(null, null, null, false, transformationPlugin, transformAlias, transformVersion, pluginLoaderSwapper);
    }

    TransformationStage(
        Plugin<Predicate<R>> predicatePlugin,
        String predicateAlias,
        String predicateVersion,
        boolean negate,
        Plugin<Transformation<R>> transformationPlugin,
        String transformAlias,
        String transformVersion,
        Function<ClassLoader, LoaderSwap> pluginLoaderSwapper
    ) {
        this.predicatePlugin = predicatePlugin;
        this.negate = negate;
        this.transformationPlugin = transformationPlugin;
        this.pluginLoaderSwapper = pluginLoaderSwapper;
        this.transformAlias = transformAlias;
        this.predicateAlias = predicateAlias;
        this.transformVersion = transformVersion;
        this.predicateVersion = predicateVersion;
    }

    public Class<? extends Transformation<R>> transformClass() {
        @SuppressWarnings("unchecked")
        Class<? extends Transformation<R>> transformClass = (Class<? extends Transformation<R>>) transformationPlugin.get().getClass();
        return transformClass;
    }

    public R apply(R record) {
        Predicate<R> predicate = predicatePlugin != null ? predicatePlugin.get() : null;
        boolean shouldTransform = predicate == null;
        if (predicate != null) {
            try (LoaderSwap swap = pluginLoaderSwapper.apply(predicate.getClass().getClassLoader())) {
                shouldTransform = negate ^ predicate.test(record);
            }
        }
        if (shouldTransform) {
            try (LoaderSwap swap = pluginLoaderSwapper.apply(transformationPlugin.get().getClass().getClassLoader())) {
                record = transformationPlugin.get().apply(record);
            }
        }
        return record;
    }

    @Override
    public void close() {
        Utils.closeQuietly(transformationPlugin, "transformation");
        Utils.closeQuietly(predicatePlugin, "predicate");
    }

    @Override
    public String toString() {
        return "TransformationStage{" +
                "predicate=" + predicatePlugin.get() +
                ", transformation=" + transformationPlugin.get() +
                ", negate=" + negate +
                '}';
    }

    public record AliasedPluginInfo(String alias, String className, String version) {
        public AliasedPluginInfo {
            Objects.requireNonNull(alias, "alias cannot be null");
            Objects.requireNonNull(className, "className cannot be null");
        }
    }


    public record StageInfo(AliasedPluginInfo transform, AliasedPluginInfo predicate) {
        public StageInfo {
            Objects.requireNonNull(transform, "transform cannot be null");
        }
    }


    public StageInfo transformationStageInfo() {
        AliasedPluginInfo transformInfo = new AliasedPluginInfo(
            transformAlias,
            transformationPlugin.get().getClass().getName(),
            transformVersion
        );
        AliasedPluginInfo predicateInfo = predicatePlugin != null ? new AliasedPluginInfo(
            predicateAlias,
            predicatePlugin.get().getClass().getName(), predicateVersion
        ) : null;
        return new StageInfo(transformInfo, predicateInfo);
    }
}
