/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.errors;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Indicates a pre-run time error incurred while parsing the {@link org.apache.kafka.streams.processor.TopologyBuilder
 * builder} to construct the {@link org.apache.kafka.streams.processor.internals.ProcessorTopology processor topology}.
 */
@InterfaceStability.Unstable
public class TopologyBuilderException extends StreamsException {

    private static final long serialVersionUID = 1L;

    public TopologyBuilderException(final String message) {
        super("Invalid topology building" + (message == null ? "" : ": " + message));
    }

    public TopologyBuilderException(final String message, final Throwable throwable) {
        super("Invalid topology building" + (message == null ? "" : ": " + message), throwable);
    }

    public TopologyBuilderException(final Throwable throwable) {
        super(throwable);
    }
}
