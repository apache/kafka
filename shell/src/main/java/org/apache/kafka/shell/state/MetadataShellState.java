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

package org.apache.kafka.shell.state;

import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.shell.node.RootShellNode;

import java.util.function.Consumer;

/**
 * The mutable state of the Kafka metadata shell.
 */
public class MetadataShellState {
    private volatile MetadataNode root;
    private volatile String workingDirectory;

    public MetadataShellState() {
        this.root = new RootShellNode(MetadataImage.EMPTY);
        this.workingDirectory = "/";
    }

    public MetadataNode root() {
        return root;
    }

    public void setRoot(MetadataNode root) {
        this.root = root;
    }

    public String workingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public void visit(Consumer<MetadataShellState> consumer) {
        consumer.accept(this);
    }
}
