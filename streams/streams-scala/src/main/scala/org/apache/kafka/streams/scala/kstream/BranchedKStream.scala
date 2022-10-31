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
package org.apache.kafka.streams.scala.kstream

import java.util

import org.apache.kafka.streams.kstream
import org.apache.kafka.streams.kstream.{BranchedKStream => BranchedKStreamJ}
import org.apache.kafka.streams.scala.FunctionsCompatConversions.PredicateFromFunction

import scala.jdk.CollectionConverters._

/**
 * Branches the records in the original stream based on the predicates supplied for the branch definitions.
 * <p>
 * Branches are defined with [[branch]] or [[defaultBranch]] methods. Each record is evaluated against the predicates
 * supplied via [[Branched]] parameters, and is routed to the first branch for which its respective predicate
 * evaluates to `true`. If a record does not match any predicates, it will be routed to the default branch,
 * or dropped if no default branch is created.
 * <p>
 *
 * Each branch (which is a [[KStream]] instance) then can be processed either by
 * a function or a consumer provided via a [[Branched]]
 * parameter. If certain conditions are met, it also can be accessed from the `Map` returned by
 * an optional [[defaultBranch]] or [[noDefaultBranch]] method call.
 * <p>
 * The branching happens on a first match basis: A record in the original stream is assigned to the corresponding result
 * stream for the first predicate that evaluates to true, and is assigned to this stream only. If you need
 * to route a record to multiple streams, you can apply multiple [[KStream.filter]] operators to the same [[KStream]]
 * instance, one for each predicate, instead of branching.
 * <p>
 * The process of routing the records to different branches is a stateless record-by-record operation.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 */
class BranchedKStream[K, V](val inner: BranchedKStreamJ[K, V]) {

  /**
   * Define a branch for records that match the predicate.
   *
   * @param predicate A predicate against which each record will be evaluated.
   *                  If this predicate returns `true` for a given record, the record will be
   *                  routed to the current branch and will not be evaluated against the predicates
   *                  for the remaining branches.
   * @return `this` to facilitate method chaining
   */
  def branch(predicate: (K, V) => Boolean): BranchedKStream[K, V] = {
    inner.branch(predicate.asPredicate)
    this
  }

  /**
   * Define a branch for records that match the predicate.
   *
   * @param predicate A predicate against which each record will be evaluated.
   *                  If this predicate returns `true` for a given record, the record will be
   *                  routed to the current branch and will not be evaluated against the predicates
   *                  for the remaining branches.
   * @param branched  A [[Branched]] parameter, that allows to define a branch name, an in-place
   *                  branch consumer or branch mapper (see <a href="#examples">code examples</a>
   *                  for [[BranchedKStream]])
   * @return `this` to facilitate method chaining
   */
  def branch(predicate: (K, V) => Boolean, branched: Branched[K, V]): BranchedKStream[K, V] = {
    inner.branch(predicate.asPredicate, branched)
    this
  }

  /**
   * Finalize the construction of branches and defines the default branch for the messages not intercepted
   * by other branches. Calling [[defaultBranch]] or [[noDefaultBranch]] is optional.
   *
   * @return Map of named branches. For rules of forming the resulting map, see [[BranchedKStream]]
   *         description.
   */
  def defaultBranch(): Map[String, KStream[K, V]] = toScalaMap(inner.defaultBranch())

  /**
   * Finalize the construction of branches and defines the default branch for the messages not intercepted
   * by other branches. Calling [[defaultBranch]] or [[noDefaultBranch]] is optional.
   *
   * @param branched A [[Branched]] parameter, that allows to define a branch name, an in-place
   *                 branch consumer or branch mapper for [[BranchedKStream]].
   * @return Map of named branches. For rules of forming the resulting map, see [[BranchedKStream]]
   *         description.
   */
  def defaultBranch(branched: Branched[K, V]): Map[String, KStream[K, V]] = toScalaMap(inner.defaultBranch(branched))

  /**
   * Finalizes the construction of branches without forming a default branch.
   *
   * @return Map of named branches. For rules of forming the resulting map, see [[BranchedKStream]]
   *         description.
   */
  def noDefaultBranch(): Map[String, KStream[K, V]] = toScalaMap(inner.noDefaultBranch())

  private def toScalaMap(m: util.Map[String, kstream.KStream[K, V]]): collection.immutable.Map[String, KStream[K, V]] =
    m.asScala.map { case (name, kStreamJ) =>
      (name, new KStream(kStreamJ))
    }.toMap
}
