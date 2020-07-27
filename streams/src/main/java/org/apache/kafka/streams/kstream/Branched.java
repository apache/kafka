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

    private final String name;
    private final Function<? super KStream<? super K, ? super V>,
            ? extends KStream<? extends K, ? extends V>> chainFunction;
    private final Consumer<? super KStream<? super K, ? super V>> chainConsumer;

    private Branched(final String name,
                     final Function<? super KStream<? super K, ? super V>, ? extends KStream<? extends K, ? extends V>> chainFunction,
                     final Consumer<? super KStream<? super K, ? super V>> chainConsumer) {
        this.name = name;
        this.chainFunction = chainFunction;
        this.chainConsumer = chainConsumer;
    }

    /**
     * Configure the instance of {@link Branched} with a branch name postfix.
     *
     * @param name the branch name postfix to be used. If {@code null} a default branch name postfix will be generated (see
     *             {@link BranchedKStream} description for details)
     * @return {@code this}
     */
    @Override
    public Branched<K, V> withName(final String name) {
        return new Branched<>(name, chainFunction, chainConsumer);
    }

    /**
     * Create an instance of {@link Branched} with provided branch name postfix.
     *
     * @param name the branch name postfix to be used. If {@code null}, a default branch name postfix will be generated
     *             (see {@link BranchedKStream} description for details)
     * @param <K>  key type
     * @param <V>  value type
     * @return a new instance of {@link Branched}
     */
    public static <K, V> Branched<K, V> as(final String name) {
        return new Branched<>(name, null, null);
    }

    /**
     * Create an instance of {@link Branched} with provided chain function.
     *
     * @param chain A function that will be applied to the branch. If {@code null}, the identity
     *              {@code kStream -> kStream} function will be supposed. If this function returns
     *              {@code null}, its result is ignored, otherwise it is added to the {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details).
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@link Branched}
     */
    @SuppressWarnings("overloads")
    public static <K, V> Branched<K, V> with(
            final Function<? super KStream<? super K, ? super V>,
                    ? extends KStream<? extends K, ? extends V>> chain) {
        return new Branched<>(null, chain, null);
    }

    /**
     * Create an instance of {@link Branched} with provided chain consumer.
     *
     * @param chain A consumer to which the branch will be sent. If a non-null branch is provided here,
     *              the respective branch will not be added to the resulting {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details). If {@code null}, a no-op consumer will be supposed
     *              and the branch will be added to the resulting {@code Map}.
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@link Branched}
     */
    @SuppressWarnings("overloads")
    public static <K, V> Branched<K, V> with(final Consumer<? super KStream<? super K, ? super V>> chain) {
        return new Branched<>(null, null, chain);
    }

    /**
     * Create an instance of {@link Branched} with provided chain function and branch name postfix.
     *
     * @param chain A function that will be applied to the branch. If {@code null}, the identity
     *              {@code kStream -> kStream} function will be supposed. If this function returns
     *              {@code null}, its result is ignored, otherwise it is added to the {@code Map} returned
     *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *              {@link BranchedKStream} description for details).
     * @param name  the branch name postfix to be used. If {@code null}, a default branch name postfix will be generated
     *              (see {@link BranchedKStream} description for details)
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@link Branched}
     */
    @SuppressWarnings("overloads")
    public static <K, V> Branched<K, V> with(
            final Function<? super KStream<? super K, ? super V>,
                    ? extends KStream<? extends K, ? extends V>> chain, final String name) {
        return new Branched<>(name, chain, null);
    }

    /**
     * Create an instance of {@link Branched} with provided chain function and branch name postfix.
     *
     * @param chain A consumer to which the branch will be sent. If a non-null branch is provided here,
     *      *              the respective branch will not be added to the resulting {@code Map} returned
     *      *              by {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} (see
     *      *              {@link BranchedKStream} description for details). If {@code null}, a no-op consumer will be supposed
     *      *              and the branch will be added to the resulting {@code Map}.
     * @param name  the branch name postfix to be used. If {@code null}, a default branch name postfix will be generated
     *              (see {@link BranchedKStream} description for details)
     * @param <K>   key type
     * @param <V>   value type
     * @return a new instance of {@link Branched}
     */
    @SuppressWarnings("overloads")
    public static <K, V> Branched<K, V> with(final Consumer<? super KStream<? super K, ? super V>> chain,
                                             final String name) {
        return new Branched<>(name, null, chain);
    }
}
