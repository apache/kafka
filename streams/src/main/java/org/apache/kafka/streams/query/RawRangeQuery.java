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

package org.apache.kafka.streams.query;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Optional;

public class RawRangeQuery implements Query<KeyValueIterator<Bytes, byte[]>> {

 private final Optional<Bytes> lower;
 private final Optional<Bytes> upper;

 private RawRangeQuery(final Optional<Bytes> lower, final Optional<Bytes> upper) {
  this.lower = lower;
  this.upper = upper;
 }

 public static RawRangeQuery withRange(final Bytes lower, final Bytes upper) {
  return new RawRangeQuery(Optional.of(lower), Optional.of(upper));
 }

 public static RawRangeQuery withUpperBound(final Bytes upper) {
  return new RawRangeQuery(Optional.empty(), Optional.of(upper));
 }

 public static RawRangeQuery withLowerBound(final Bytes lower) {
  return new RawRangeQuery(Optional.of(lower), Optional.empty());
 }

 public static RawRangeQuery withNoBounds() {
  return new RawRangeQuery(Optional.empty(), Optional.empty());
 }

 public Optional<Bytes> getLowerBound() {
  return lower;
 }

 public Optional<Bytes> getUpperBound() {
  return upper;
 }
}