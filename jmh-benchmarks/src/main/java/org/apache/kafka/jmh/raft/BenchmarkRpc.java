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
 * A single inbound-RPC scenario for the raft request-handling benchmarks: the request to deliver to
 * the node under test, together with the RPCs the node should have left on its send queue once it has
 * handled that request.
 *
 * Each scenario is a constant of an enum that implements this interface, and a benchmark method takes
 * that enum as a JMH {@code @Param}, so JMH runs the method once per constant. Every constant is its
 * own result row and its own regression baseline, and adding a scenario means adding a constant.
 *
 * A benchmark times only the handling of the request: {@link #build} constructs the request in setup,
 * outside the measured region. {@link #expectedRequest()} and {@link #expectedResponse()} declare the
 * RPCs the node should have left on its send queue afterward; the harness drains exactly those and
 * fails if anything else remains, catching an unintended extra RPC.
 */
public interface BenchmarkRpc {

    ApiMessage build(RaftClientBenchmarkContext benchmark);

    Optional<ApiKeys> expectedRequest();

    Optional<ApiKeys> expectedResponse();
}
