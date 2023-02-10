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
 * {@link BranchedKStream#defaultBranch(Branched)} methods. Each record is evaluated against the {@code predicate}
 * supplied via {@link Branched} parameters, and is routed to the first branch for which its respective predicate
 * evaluates to {@code true}. If a record does not match any predicates, it will be routed to the default branch,
 * or dropped if no default branch is created.
 * <p>
 * Each branch (which is a {@link KStream} instance) then can be processed either by
 * a {@link java.util.function.Function} or a {@link java.util.function.Consumer} provided via a {@link Branched}
 * parameter. If certain conditions are met, it also can be accessed from the {@link Map} returned by an optional
 * {@link BranchedKStream#defaultBranch(Branched)} or {@link BranchedKStream#noDefaultBranch()} method call
 * (see <a href="#examples">usage examples</a>).
 * <p>
 * The branching happens on a first-match basis: A record in the original stream is assigned to the corresponding result
 * stream for the first predicate that evaluates to {@code true}, and is assigned to this stream only. If you need
 * to route a record to multiple streams, you can apply multiple {@link KStream#filter(Predicate)} operators
 * to the same {@link KStream} instance, one for each predicate, instead of branching.
 * <p>
 * The process of routing the records to different branches is a stateless record-by-record operation.
 *
 * <h2><a name="maprules">Rules of forming the resulting map</a></h2>
 * The keys of the {@code Map<String, KStream<K, V>>} entries returned by {@link BranchedKStream#defaultBranch(Branched)} or
 * {@link BranchedKStream#noDefaultBranch()} are defined by the following rules:
 * <ul>
 *     <li>If {@link Named} parameter was provided for {@link KStream#split(Named)}, its value is used as
 *     a prefix for each key. By default, no prefix is used
 *     <li>If a branch name is provided in {@link BranchedKStream#branch(Predicate, Branched)} via the
 *     {@link Branched} parameter, its value is appended to the prefix to form the {@code Map} key
 *     <li>If a name is not provided for the branch, then the key defaults to {@code prefix + position} of the branch
 *     as a decimal number, starting from {@code "1"}
 *     <li>If a name is not provided for the {@link BranchedKStream#defaultBranch()}, then the key defaults
 *     to {@code prefix + "0"}
 * </ul>
 * The values of the respective {@code Map<Stream, KStream<K, V>>} entries are formed as following:
 * <ul>
 *     <li>If no chain function or consumer is provided in {@link BranchedKStream#branch(Predicate, Branched)} via
 *     the {@link Branched} parameter, then the branch itself is added to the {@code Map}
 *     <li>If chain function is provided and it returns a non-null value for a given branch, then the value
 *     is the result returned by this function
 *     <li>If a chain function returns {@code null} for a given branch, then no entry is added to the map
 *     <li>If a consumer is provided for a given branch, then no entry is added to the map
 * </ul>
 * For example:
 * <pre> {@code
 * Map<String, KStream<..., ...>> result =
 *   source.split(Named.as("foo-"))
 *     .branch(predicate1, Branched.as("bar"))                    // "foo-bar"
 *     .branch(predicate2, Branched.withConsumer(ks->ks.to("A"))  // no entry: a Consumer is provided
 *     .branch(predicate3, Branched.withFunction(ks->null))       // no entry: chain function returns null
 *     .branch(predicate4, Branched.withFunction(ks->ks))         // "foo-4": chain function returns non-null value
 *     .branch(predicate5)                                        // "foo-5": name defaults to the branch position
 *     .defaultBranch()                                           // "foo-0": "0" is the default name for the default branch
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
 *     .branch(predicate1, Branched.withConsumer(ks -> ks.to("A")))
 *     .branch(predicate2, Branched.withConsumer(ks -> ks.to("B")))
 *     .defaultBranch(Branched.withConsumer(ks->ks.to("C")));
 * }</pre>
 *
 * <h3>Collecting branches in a single scope</h3>
 * In other cases we want to combine branches again after splitting. The map returned by
 * {@link BranchedKStream#defaultBranch()} or {@link BranchedKStream#noDefaultBranch()} methods provides
 * access to all the branches in the same scope:
 *
 * <pre> {@code
 * Map<String, KStream<String, String>> branches = source.split(Named.as("split-"))
 *     .branch((key, value) -> value == null, Branched.withFunction(s -> s.mapValues(v->"NULL"), "null")
 *     .defaultBranch(Branched.as("non-null"));
 *
 * KStream<String, String> merged = branches.get("split-non-null").merge(branches.get("split-null"));
 * }</pre>
 *
 * <h3>Dynamic branching</h3>
 * There is also a case when we might need to create branches dynamically, e. g. one per enum value:
 *
 * <pre> {@code
 * BranchedKStream branched = stream.split();
 * for (RecordType recordType : RecordType.values())
 *     branched.branch((k, v) -> v.getRecType() == recordType,
 *         Branched.withConsumer(recordType::processRecords));
 * }</pre>
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KStream
 */
public interface BranchedKStream<K, V> {
    /**
     * Define a branch for records that match the predicate.
     *
     * @param predicate A {@link Predicate} instance, against which each record will be evaluated.
     *                  If this predicate returns {@code true} for a given record, the record will be
     *                  routed to the current branch and will not be evaluated against the predicates
     *                  for the remaining branches.
     * @return {@code this} to facilitate method chaining
     */
    BranchedKStream<K, V> branch(Predicate<? super K, ? super V> predicate);

    /**
     * Define a branch for records that match the predicate.
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
     * Finalize the construction of branches and defines the default branch for the messages not intercepted
     * by other branches. Calling {@code defaultBranch} or {@link #noDefaultBranch()} is optional.
     *
     * @return {@link Map} of named branches. For rules of forming the resulting map, see {@code BranchedKStream}
     * <a href="#maprules">description</a>.
     */
    Map<String, KStream<K, V>> defaultBranch();

    /**
     * Finalize the construction of branches and defines the default branch for the messages not intercepted
     * by other branches. Calling {@code defaultBranch} or {@link #noDefaultBranch()} is optional.
     *
     * @param branched A {@link Branched} parameter, that allows to define a branch name, an in-place
     *                 branch consumer or branch mapper (see <a href="#examples">code examples</a>
     *                 for {@link BranchedKStream})
     * @return {@link Map} of named branches. For rules of forming the resulting map, see {@link BranchedKStream}
     * <a href="#maprules">description</a>.
     */
    Map<String, KStream<K, V>> defaultBranch(Branched<K, V> branched);

    /**
     * Finalize the construction of branches without forming a default branch.  Calling {@code #noDefaultBranch()}
     * or {@link #defaultBranch()} is optional.
     *
     * @return {@link Map} of named branches. For rules of forming the resulting map, see {@link BranchedKStream}
     * <a href="#maprules">description</a>.
     */
    Map<String, KStream<K, V>> noDefaultBranch();
}
