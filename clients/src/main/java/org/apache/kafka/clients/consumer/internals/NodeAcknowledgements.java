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
package org.apache.kafka.clients.consumer.internals;

import java.util.Objects;

/**
 * This class combines Acknowledgements with the id of the node to use for acknowledging.
 */
public class NodeAcknowledgements {
    private final int nodeId;
    private final Acknowledgements acknowledgements;

    public NodeAcknowledgements(int nodeId, Acknowledgements acknowledgements) {
        this.nodeId = nodeId;
        this.acknowledgements = Objects.requireNonNull(acknowledgements);
    }

    public int nodeId() {
        return nodeId;
    }

    public Acknowledgements acknowledgements() {
        return acknowledgements;
    }
}
