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
package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Properties;

/**
 *  This class allows the instantiation of a {@link TopologyTestDriver} using a
 *  {@link InternalTopologyBuilder} by exposing a protected constructor.
 *
 *  It should be used only for testing, and should be removed once the deprecated
 *  classes {@link org.apache.kafka.streams.kstream.KStreamBuilder} and
 *  {@link org.apache.kafka.streams.processor.TopologyBuilder} are removed.
 */
public class TopologyTestDriverWrapper extends TopologyTestDriver {

    public TopologyTestDriverWrapper(final InternalTopologyBuilder builder,
                                     final Properties config) {
        super(builder, config);
    }
}
