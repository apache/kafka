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


import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.PluginVersionUtils;

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
    private final Predicate<R> predicate;
    private final Transformation<R> transformation;
    private final boolean negate;
    private final String transformAlias;
    private final String predicateAlias;
    private final Function<ClassLoader, LoaderSwap> pluginLoaderSwapper;

    TransformationStage(Transformation<R> transformation) {
        this(null, null, false, transformation, null, PluginUtils.noOpLoaderSwap());
    }

    TransformationStage(Transformation<R> transformation, String transformAlias, Function<ClassLoader, LoaderSwap> pluginLoaderSwapper) {
        this(null, null, false, transformation, transformAlias, pluginLoaderSwapper);
    }

    TransformationStage(Predicate<R> predicate, boolean negate, Transformation<R> transformation) {
        this(predicate, null, negate, transformation, null, PluginUtils.noOpLoaderSwap());
    }

    TransformationStage(Predicate<R> predicate,
                        String predicateAlias,
                        boolean negate,
                        Transformation<R> transformation,
                        String transformAlias,
                        Function<ClassLoader, LoaderSwap> pluginLoaderSwapper) {
        this.predicate = predicate;
        this.negate = negate;
        this.transformation = transformation;
        this.pluginLoaderSwapper = pluginLoaderSwapper;
        this.transformAlias = transformAlias;
        this.predicateAlias = predicateAlias;
    }

    public Class<? extends Transformation<R>> transformClass() {
        @SuppressWarnings("unchecked")
        Class<? extends Transformation<R>> transformClass = (Class<? extends Transformation<R>>) transformation.getClass();
        return transformClass;
    }

    public R apply(R record) {
        boolean shouldTransforms = predicate == null;
        if (predicate != null) {
            try (LoaderSwap swap = pluginLoaderSwapper.apply(predicate.getClass().getClassLoader())) {
                shouldTransforms = negate ^ predicate.test(record);
            }
        }
        if (shouldTransforms) {
            try (LoaderSwap swap = pluginLoaderSwapper.apply(transformation.getClass().getClassLoader())) {
                record = transformation.apply(record);
            }
        }
        return record;
    }

    @Override
    public void close() {
        Utils.closeQuietly(transformation, "transformation");
        Utils.closeQuietly(predicate, "predicate");
    }

    @Override
    public String toString() {
        return "TransformationStage{" +
                "predicate=" + predicate +
                ", transformation=" + transformation +
                ", negate=" + negate +
                '}';
    }

    public static class AliasedPluginInfo {
        private final String alias;
        private final String className;
        private final String version;

        private AliasedPluginInfo(String alias, String className, String version) {
            this.alias = alias;
            this.className = className;
            this.version = version;
        }

        public String alias() {
            return alias;
        }

        public String className() {
            return className;
        }

        public String version() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AliasedPluginInfo that = (AliasedPluginInfo) o;
            return Objects.equals(alias, that.alias) &&
                    Objects.equals(className, that.className) &&
                    Objects.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, className, version);
        }
    }

    public static class StageInfo {
        private final AliasedPluginInfo transform;
        private final AliasedPluginInfo predicate;

        private StageInfo(AliasedPluginInfo transform, AliasedPluginInfo predicate) {
            this.transform = transform;
            this.predicate = predicate;
        }

        public AliasedPluginInfo transform() {
            return transform;
        }

        public AliasedPluginInfo predicate() {
            return predicate;
        }
    }

    public StageInfo info() {
        AliasedPluginInfo transformInfo = new AliasedPluginInfo(transformAlias,
                transformation.getClass().getName(), PluginVersionUtils.getVersionOrUndefined(transformation, pluginLoaderSwapper));
        AliasedPluginInfo predicateInfo = predicate != null ? new AliasedPluginInfo(predicateAlias,
                predicate.getClass().getName(), PluginVersionUtils.getVersionOrUndefined(predicate, pluginLoaderSwapper)) : null;
        return new StageInfo(transformInfo, predicateInfo);
    }
}
