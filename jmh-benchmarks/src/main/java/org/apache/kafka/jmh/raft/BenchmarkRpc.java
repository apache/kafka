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
package org.apache.kafka.jmh.raft;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.raft.RaftClientBenchmarkContext;

import java.util.Optional;

/**
 * <p>This is the contract the raft benchmarks parametrize over. A scenario is a constant in an enum
 * that implements this interface; a benchmark method takes that enum as a JMH {@code @Param} with no
 * explicit value list, so JMH sweeps every constant and each constant becomes its own result row and
 * its own regression baseline. Adding a scenario is therefore adding one enum constant, no new method
 * or wiring.
 *
 * <p>A constant supplies two things:
 * <ul>
 *   <li>{@link #build} constructs the inbound request the benchmark delivers. It is called once in
 *       benchmark setup, so building the request is not part of the measured region.</li>
 *   <li>{@link #expectedRequest()} / {@link #expectedResponse()} declare the request/response API keys
 *       the node under test should still have in-flight when the invocation ends. The harness drains
 *       exactly those and asserts nothing else remains, so a refactor that starts emitting an extra
 *       RPC fails fast instead of quietly skewing the score. An empty {@link Optional} means none are
 *       expected.</li>
 * </ul>
 *
 * <p>Scenarios are split across more than one enum by JMH mode rather than collected into a single
 * enum, because the mode is fixed per benchmark method: read-only RPCs reuse one prepared node in
 * {@code AverageTime}, while state-mutating RPCs need a fresh node per invocation in
 * {@code SingleShotTime}.
 */
public interface BenchmarkRpc {

    /** Builds the inbound request this scenario delivers to the node under test. */
    ApiMessage build(RaftClientBenchmarkContext benchmark);

    /** The request API key expected to still be in-flight when the invocation ends, if any. */
    Optional<ApiKeys> expectedRequest();

    /** The response API key expected to still be in-flight when the invocation ends, if any. */
    Optional<ApiKeys> expectedResponse();
}
