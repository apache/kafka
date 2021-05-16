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
package org.apache.kafka.streams.kstream;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The {@code Branched} class is used to define the optional parameters when building branches with
 * {@link BranchedKStream}.
 *
 * @param <K> type of record key
 * @param <V> type of record value
 */
public class Branched<K, V> implements NamedOperation<Branched<K, V>> {

    protected final String name;
    protected final Function<? super KStream<K, V>, ? extends KStream<K, V>> chainFunction;
    protected final Consumer<? super KStream<K, V>> chainConsumer;

    protected Branched(final String name,
                       final Function<? super KStream<K, V>, ? extends KStream<K, V>> chainFunction,
                       final Consumer<? super KStream<K, V>> chainConsumer) {
        this.name = name;
        this.chainFunction = chainFunction;
        this.chainConsumer = chainConsumer;
    }

    /**
     * Create an instance of {@code Branched} with provided branch name suffix.
     *
     * @param name the branch name suffix to be used (see {@link BranchedKStream} description for details)
     * @param <K>  key type
     * @param <V>  value type
     * @return a new instance of {@code Branched}
     */
    public static <K, V> Branched<K, V> as(final String name) {
        Objects.requireNonNull(name, "name cannot be null");
        return new Branched<>(name, null, null);
    }

    /**
     * Create an instance of {@code Branched} with provided chain function.
     *
     * @param chain A function that will be applied to the branch. If the provided function returns
     *              {@code null}, its result is ignored, otherwise it is added to the {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details).
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@code Branched}
     */
    public static <K, V> Branched<K, V> withFunction(
            final Function<? super KStream<K, V>, ? extends KStream<K, V>> chain) {
        Objects.requireNonNull(chain, "chain function cannot be null");
        return new Branched<>(null, chain, null);
    }

    /**
     * Create an instance of {@code Branched} with provided chain consumer.
     *
     * @param chain A consumer to which the branch will be sent. If a consumer is provided,
     *              the respective branch will not be added to the resulting {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details).
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@code Branched}
     */
    public static <K, V> Branched<K, V> withConsumer(final Consumer<KStream<K, V>> chain) {
        Objects.requireNonNull(chain, "chain consumer cannot be null");
        return new Branched<>(null, null, chain);
    }

    /**
     * Create an instance of {@code Branched} with provided chain function and branch name suffix.
     *
     * @param chain A function that will be applied to the branch. If the provided function returns
     *              {@code null}, its result is ignored, otherwise it is added to the {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details).
     * @param name  the branch name suffix to be used. If {@code null}, a default branch name suffix will be generated
     *              (see {@link BranchedKStream} description for details)
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@code Branched}
     */
    public static <K, V> Branched<K, V> withFunction(
            final Function<? super KStream<K, V>, ? extends KStream<K, V>> chain, final String name) {
        Objects.requireNonNull(chain, "chain function cannot be null");
        return new Branched<>(name, chain, null);
    }

    /**
     * Create an instance of {@code Branched} with provided chain consumer and branch name suffix.
     *
     * @param chain A consumer to which the branch will be sent. If a non-null consumer is provided,
     *              the respective branch will not be added to the resulting {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details).
     * @param name  the branch name suffix to be used. If {@code null}, a default branch name suffix will be generated
     *              (see {@link BranchedKStream} description for details)
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@code Branched}
     */
    public static <K, V> Branched<K, V> withConsumer(final Consumer<? super KStream<K, V>> chain,
                                                     final String name) {
        Objects.requireNonNull(chain, "chain consumer cannot be null");
        return new Branched<>(name, null, chain);
    }

    /**
     * Create an instance of {@code Branched} from an existing instance.
     *
     * @param branched the instance of {@code Branched} to copy
     */
    protected Branched(final Branched<K, V> branched) {
        this(branched.name, branched.chainFunction, branched.chainConsumer);
    }

    /**
     * Configure the instance of {@code Branched} with a branch name suffix.
     *
     * @param name the branch name suffix to be used. If {@code null} a default branch name suffix will be generated (see
     *             {@link BranchedKStream} description for details)
     * @return {@code this}
     */
    @Override
    public Branched<K, V> withName(final String name) {
        return new Branched<>(name, chainFunction, chainConsumer);
    }
}
