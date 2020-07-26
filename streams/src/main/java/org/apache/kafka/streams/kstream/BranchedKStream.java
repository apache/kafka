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

import java.util.Map;

/**
 * Branches the records in the original stream based on the predicates supplied for the branch definitions.
 * <p>
 * Branches are defined with {@link BranchedKStream#branch(Predicate, Branched)} or
 * {@link BranchedKStream#defaultBranch(Branched)} methods. Each record is evaluated against the predicates
 * supplied via {@link Branched} parameters, and is routed to the first branch for which its respective predicate
 * evaluates to {@code true}.
 * <p>
 * Each branch (which is a {@link KStream} instance) then can be processed either by
 * a {@link java.util.function.Function} or a {@link java.util.function.Consumer} provided via a {@link Branched}
 * parameter. It also can be accessed from the {@link Map} returned by {@link BranchedKStream#defaultBranch(Branched)} or
 * {@link BranchedKStream#noDefaultBranch()} (see <a href="#examples">usage examples</a>).
 * <p>
 * The branching happens on first-match: A record in the original stream is assigned to the corresponding result
 * stream for the first predicate that evaluates to true, and is assigned to this stream only. If you need
 * to route a record to multiple streams, you can use {@link KStream#filter(Predicate)} for each predicate instead
 * of branching.
 * <p>
 * The process of routing the records to different branches is a stateless record-by-record operation.
 * <h2><a name="maprules">Rules of forming the resulting map</a></h2>
 * The keys of the {@code Map<String, KStream<K, V>>} entries returned by {@link BranchedKStream#defaultBranch(Branched)} or
 * {@link BranchedKStream#noDefaultBranch()} are defined by the following rules:
 * <p>
 * <ul>
 *     <li>If {@link Named} parameter was provided for {@link KStream#split(Named)}, its value is used as
 *     a prefix for each key. By default, no prefix is used
 *     <li>If a name is provided for the {@link BranchedKStream#branch(Predicate, Branched)} via
 *     {@link Branched} parameter, its value is appended to the prefix to form the {@code Map} key
 *     <li>If a name is not provided for the branch, then the key defaults to {@code prefix + position} of the branch
 *     as a decimal number, starting from {@code "1"}
 *     <li>If a name is not provided for the {@link BranchedKStream#defaultBranch()} call, then the key defaults
 *     to {@code prefix + "0"}
 * </ul>
 * <p>
 * The values of the respective {@code Map<Stream, KStream<K, V>>} entries are formed as following:
 * <p>
 * <ul>
 *     <li>If no chain function or consumer is provided {@link BranchedKStream#branch(Predicate, Branched)} via
 *     {@link Branched} parameter, then the value is the branch itself (which is equivalent to {@code ks -> ks}
 *     identity chain function)
 *     <li>If a chain function is provided and returns a non-null value for a given branch, then the value is
 *     the result returned by this function
 *     <li>If a chain function returns {@code null} for a given branch, then the respective entry is not put to the map.
 *     <li>If a consumer is provided for a given branch, then the the respective entry is not put to the map
 * </ul>
 * <p>
 * For example:
 * <pre> {@code
 * Map<String, KStream<..., ...>> result =
 *     source.split(Named.as("foo-"))
 *         .branch(predicate1, Branched.as("bar"))            // "foo-bar"
 *         .branch(predicate2, Branched.with(ks->ks.to("A"))  // no entry: a Consumer is provided
 *         .branch(predicate3, Branched.with(ks->null))       // no entry: chain function returns null
 *         .branch(predicate4)                                // "foo-4": name defaults to the branch position
 *         .defaultBranch()                                   // "foo-0": "0" is the default name for the default branch
 * }</pre>
 *
 * <h2><a name="examples">Usage examples</a></h2>
 *
 * <h3>Direct Branch Consuming</h3>
 * In many cases we do not need to have a single scope for all the branches, each branch being processed completely
 * independently from others. Then we can use 'consuming' lambdas or method references in {@link Branched} parameter:
 *
 * <pre> {@code
 * source.split()
 *     .branch((key, value) -> value.contains("A"), Branched.with(ks -> ks.to("A")))
 *     .branch((key, value) -> value.contains("B"), Branched.with(ks -> ks.to("B")))
 *     .defaultBranch(Branched.with(ks->ks.to("C")));
 * }</pre>
 *
 * <h3>Collecting branches in a single scope</h3>
 * In other cases we want to combine branches again after splitting. The map returned by
 * {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} methods provides
 * access to all the branches in the same scope:
 *
 * <pre> {@code
 * Map<String, KStream<String, String>> branches = source.split()
 *     .branch((key, value) -> value == null, Branched.with(s -> s.mapValues(v->"NULL"), "null")
 *     .defaultBranch(Branched.as("non-null"));
 *
 * KStream<String, String> merged = branches.get("non-null").merge(branches.get("null"));
 * }</pre>
 *
 * <h3>Dynamic branching</h3>
 * There is also a case when we might need to create branches dynamically, e. g. one per enum value:
 *
 * <pre> {@code
 * BranchedKStream branched = stream.split();
 * for (RecordType recordType : RecordType.values())
 *     branched.branch((k, v) -> v.getRecType() == recordType,
 *         Branched.with(recordType::processRecords));
 * }</pre>
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 */
public interface BranchedKStream<K, V> {
    /**
     * Defines a branch for records that match the predicate.
     *
     * @param predicate A {@link Predicate} instance, against which each record will be evaluated.
     *                  If this predicate returns {@code true} for a given record, the record will be
     *                  routed to the current branch and will not be evaluated against the predicates
     *                  for the remaining branches.
     * @return {@code this} to facilitate method chaining
     */
    BranchedKStream<K, V> branch(Predicate<? super K, ? super V> predicate);

    /**
     * Defines a branch for records that match the predicate.
     *
     * @param predicate A {@link Predicate} instance, against which each record will be evaluated.
     *                  If this predicate returns {@code true} for a given record, the record will be
     *                  routed to the current branch and will not be evaluated against the predicates
     *                  for the remaining branches.
     * @param branched  A {@link Branched} parameter, that allows to define a branch name, an in-place
     *                  branch consumer or branch mapper (see <a href="#examples">code examples</a>
     *                  for {@link BranchedKStream})
     * @return {@code this} to facilitate method chaining
     */
    BranchedKStream<K, V> branch(Predicate<? super K, ? super V> predicate, Branched<K, V> branched);

    /**
     * Finalizes the construction of branches and defines the default branch for the messages not intercepted
     * by other branches.
     *
     * @return {@link Map} of named branches. For rules of forming the resulting map, see {@link BranchedKStream}
     * <a href="#maprules">description</a>.
     */
    Map<String, KStream<K, V>> defaultBranch();

    /**
     * Finalizes the construction of branches and defines the default branch for the messages not intercepted
     * by other branches.
     *
     * @param branched A {@link Branched} parameter, that allows to define a branch name, an in-place
     *                 branch consumer or branch mapper (see <a href="#examples">code examples</a>
     *                 for {@link BranchedKStream})
     * @return {@link Map} of named branches. For rules of forming the resulting map, see {@link BranchedKStream}
     * <a href="#maprules">description</a>.
     */
    Map<String, KStream<K, V>> defaultBranch(Branched<K, V> branched);

    /**
     * Finalizes the construction of branches without forming a default branch.
     *
     * @return {@link Map} of named branches. For rules of forming the resulting map, see {@link BranchedKStream}
     * <a href="#maprules">description</a>.
     */
    Map<String, KStream<K, V>> noDefaultBranch();
}
