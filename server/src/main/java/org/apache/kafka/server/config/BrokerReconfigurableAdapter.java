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
package org.apache.kafka.server.config;

import org.apache.kafka.config.DynamicConfigurable;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public final class BrokerReconfigurableAdapter<T> implements BrokerReconfigurable {

    private final Set<String> reconfigurableConfigs;
    private final Function<AbstractKafkaConfig, T> configFactory;
    private final Consumer<T> validator;
    private final BiConsumer<T, T> reconfigurer;

    public static <T> BrokerReconfigurableAdapter<T> of(
        Set<String> reconfigurableConfigs,
        Function<AbstractKafkaConfig, T> configFactory,
        Consumer<T> validator,
        BiConsumer<T, T> reconfigurer
    ) {
        return new BrokerReconfigurableAdapter<>(reconfigurableConfigs, configFactory, validator, reconfigurer);
    }

    public static <T> BrokerReconfigurableAdapter<T> of(
        DynamicConfigurable<T> component,
        Function<AbstractKafkaConfig, T> configFactory
    ) {
        return of(
            component.reconfigurableConfigs(),
            configFactory,
            component::validateReconfiguration,
            component::reconfigure
        );
    }

    private BrokerReconfigurableAdapter(
        Set<String> reconfigurableConfigs,
        Function<AbstractKafkaConfig, T> configFactory,
        Consumer<T> validator,
        BiConsumer<T, T> reconfigurer
    ) {
        this.reconfigurableConfigs = Objects.requireNonNull(reconfigurableConfigs);
        this.configFactory = Objects.requireNonNull(configFactory);
        this.validator = Objects.requireNonNull(validator);
        this.reconfigurer = Objects.requireNonNull(reconfigurer);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return reconfigurableConfigs;
    }

    @Override
    public void validateReconfiguration(AbstractKafkaConfig newConfig) {
        validator.accept(configFactory.apply(newConfig));
    }

    @Override
    public void reconfigure(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig) {
        reconfigurer.accept(configFactory.apply(oldConfig), configFactory.apply(newConfig));
    }
}
