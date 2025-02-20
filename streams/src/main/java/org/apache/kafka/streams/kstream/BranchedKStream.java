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

import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;

/**
 * {@code BranchedKStream} is an abstraction of a <em>branched</em> record stream of {@link Record key-value} pairs.
 * It is an intermediate representation of a {@link KStream} in order to split the original {@link KStream} into
 * multiple {@link KStream sub-streams} (called branches).
 * The process of routing the records to different branches is a stateless record-by-record operation.
 *
 * <p>Branches are defined via {@link #branch(Predicate, Branched)} or {@link #defaultBranch(Branched)} methods.
 * Each input record is evaluated against the {@code predicate} supplied via {@link Branched} parameters, and is routed
 * to the <em>first</em> branch for which its respective predicate evaluates to {@code true}, and is included in this
 * branch only.
 * If a record does not match any predicates, it will be routed to the default branch, or dropped if no default branch
 * is created.
 * For details about multicasting/broadcasting records into more than one {@link KStream}, see {@link KStream#split()}.
 *
 * <p>Each {@link KStream branch} can be processed either by a {@link java.util.function.Function Function} or a
 * {@link java.util.function.Consumer Consumer} provided via a {@link Branched} parameter.
 * If certain conditions are met (see below), all created branches can be accessed from the {@link Map} returned by an
 * optional {@link #defaultBranch(Branched)} or {@link #noDefaultBranch()} method call.
 *
 * <h6>Rules of forming the resulting {@link Map}</h6>
 *
 * The keys of the {@link Map Map&lt;String, KStream&lt;K, V&gt;&gt;} entries returned by
 * {@link #defaultBranch(Branched)} or {@link #noDefaultBranch()} are defined by the following rules:
 * <ul>
 *   <li>If {@link Named} parameter was provided for {@link KStream#split(Named)}, its value is used as a prefix for each key.
 *       By default, no prefix is used.</li>
 *   <li>If a branch name is provided in {@link #branch(Predicate, Branched)} via the {@link Branched} parameter,
 *       its value is appended to the prefix to form the {@link Map} key.</li>
 *   <li>If a name is not provided for the branch, then the key defaults to {@code prefix + position} of the branch as
 *       a decimal number, starting from {@code "1"}.</li>
 *   <li>If a name is not provided for the {@link #defaultBranch()}, then the key defaults to {@code prefix + "0"}.</li>
 * </ul>
 *
 * The values of the respective {@link Map Map&lt;Stream, KStream&lt;K, V&gt;&gt;} entries are formed as following:
 * <ul>
 *   <li>If no {@link java.util.function.Function chain function} or {@link java.util.function.Consumer consumer} is
 *       provided in {@link #branch(Predicate, Branched)} via the {@link Branched} parameter,
 *       then the branch itself is added to the {@code Map}.</li>
 *   <li>If a {@link java.util.function.Function chain function} is provided, and it returns a non-{@code null} value for a given branch,
 *       then the value is the result returned by this function.</li>
 *   <li>If a {@link java.util.function.Function chain function} returns {@code null} for a given branch,
 *       then no entry is added to the {@link Map}.</li>
 *   <li>If a {@link java.util.function.Consumer consumer} is provided for a given branch,
 *       then no entry is added to the {@link Map}.</li>
 * </ul>
 *
 * For example:
 * <pre>{@code
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
 * <h4><a name="examples">Usage examples</a></h4>
 *
 * <h6>Direct branch processing</h6>
 *
 * If no single scope for all the branches is required, and each branch can be processed completely
 * independently of others, 'consuming' lambdas or method references in {@link Branched} parameter can be used:
 * <pre>{@code
 * source.split()
 *   .branch(predicate1, Branched.withConsumer(ks -> ks.to("A")))
 *   .branch(predicate2, Branched.withConsumer(ks -> ks.to("B")))
 *   .defaultBranch(Branched.withConsumer(ks->ks.to("C")));
 * }</pre>
 *
 * <h6>Collecting branches in a single scope</h6>
 *
 * If multiple branches need to be processed in the same scope, for example for merging or joining branches again after
 * splitting, the {@link Map} returned by {@link #defaultBranch()} or {@link #noDefaultBranch()} methods provides
 * access to all the branches in the same scope:
 * <pre>{@code
 * Map<String, KStream<String, String>> branches = source.split(Named.as("split-"))
 *   .branch((key, value) -> value == null, Branched.withFunction(s -> s.mapValues(v->"NULL"), "null")
 *   .defaultBranch(Branched.as("non-null"));
 *
 * KStream<String, String> merged = branches.get("split-non-null").merge(branches.get("split-null"));
 * }</pre>
 *
 * <h6>Dynamic branching</h6>
 *
 * There is also a case when dynamic branch creating is needed, e.g., one branch per enum value:
 * <pre>{@code
 * BranchedKStream branched = stream.split();
 * for (RecordType recordType : RecordType.values()) {
 *   branched.branch((k, v) -> v.getRecType() == recordType, Branched.withConsumer(recordType::processRecords));
 * }
 * }</pre>
 *
 * @param <K> the key type of this stream
 * @param <V> the value type of this stream
 */
public interface BranchedKStream<K, V> {
    /**
     * Define a branch for records that match the predicate.
     *
     * @param predicate
     *        A {@link Predicate} instance, against which each record will be evaluated.
     *        If this predicate returns {@code true} for a given record, the record will be
     *        routed to the current branch and will not be evaluated against the predicates
     *        for the remaining branches.
     *
     * @return {@code this} to facilitate method chaining
     */
    BranchedKStream<K, V> branch(Predicate<? super K, ? super V> predicate);

    /**
     * Define a branch for records that match the predicate.
     *
     * @param predicate
     *        A {@link Predicate} instance, against which each record will be evaluated.
     *        If this predicate returns {@code true} for a given record, the record will be
     *        routed to the current branch and will not be evaluated against the predicates
     *        for the remaining branches.
     * @param branched
     *        A {@link Branched} parameter, that allows to define a branch name, an in-place
     *        branch consumer or branch mapper (see <a href="#examples">code examples</a>
     *        for {@link BranchedKStream})
     *
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
     * @param branched
     *        A {@link Branched} parameter, that allows to define a branch name, an in-place
     *        branch consumer or branch mapper (see <a href="#examples">code examples</a>
     *        for {@link BranchedKStream})
     *
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
