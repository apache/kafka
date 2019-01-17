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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Provides a method-chaining way to build {@link KStream#branch branches} in
 * Kafka Streams processor topology.
 * <p>
 * Example of usage:
 * <pre>
 * {@code
 * new KafkaStreamsBrancher<String, String>()
 *    .addBranch((key, value) -> value.contains("A"), ks->ks.to("A"))
 *    .addBranch((key, value) -> value.contains("B"), ks->ks.to("B"))
 *    //default branch should not necessarily be defined in the end
 *    .addDefaultBranch(ks->ks.to("C"))
 *    .onTopOf(builder.stream("source"))
 * }
 * </pre>
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @author Ivan Ponomarev
 * @since 2.2.4
 */
public final class KafkaStreamBrancher<K, V> {

	private final List<Predicate<? super K, ? super V>> predicateList = new ArrayList<>();
	private final List<Consumer<? super KStream<K, V>>> consumerList = new ArrayList<>();
	private Consumer<? super KStream<K, V>> defaultConsumer;

	/**
	 * Defines a new branch.
	 *
	 * @param predicate {@link Predicate} instance
	 * @param consumer  The consumer of this branch's {@code KStream}
	 * @return {@code this}
	 */
	public KafkaStreamBrancher<K, V> addBranch(Predicate<? super K, ? super V> predicate,
											Consumer<? super KStream<K, V>> consumer) {
		this.predicateList.add(Objects.requireNonNull(predicate));
		this.consumerList.add(Objects.requireNonNull(consumer));
		return this;
	}

	/**
	 * Defines a default branch. To this stream will be directed all the messages that
	 * were not dispatched to other branches. This method should not necessarily be called in the end
	 * of chain.
	 *
	 * @param consumer The consumer of this branch's {@code KStream}
	 * @return {@code this}
	 */
	public KafkaStreamBrancher<K, V> addDefaultBranch(Consumer<? super KStream<K, V>> consumer) {
		this.defaultConsumer = Objects.requireNonNull(consumer);
		return this;
	}

	/**
	 * Terminating method that builds branches on top of given {@code KStream}.
	 *
	 * @param stream {@code KStream} to split
	 */
	public void onTopOf(KStream<K, V> stream) {
		if (this.defaultConsumer != null) {
			this.predicateList.add((k, v) -> true);
			this.consumerList.add(this.defaultConsumer);
		}
		@SuppressWarnings({"unchecked", "rawtypes"})
		Predicate<? super K, ? super V>[] predicates = this.predicateList.toArray(new Predicate[0]);
		KStream<K, V>[] result = stream.branch(predicates);
		for (int i = 0; i < this.consumerList.size(); i++) {
			this.consumerList.get(i).accept(result[i]);
		}
	}
}
