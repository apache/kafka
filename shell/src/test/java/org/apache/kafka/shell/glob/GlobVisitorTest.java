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

package org.apache.kafka.shell.glob;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.image.node.MetadataNode;
import org.apache.kafka.shell.glob.GlobVisitor.MetadataNodeInfo;
import org.apache.kafka.shell.state.MetadataShellState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

@Timeout(value = 5, unit = MINUTES)
public class GlobVisitorTest {
    static private final MetadataShellState DATA;

    static class TestNode implements MetadataNode {
        private final String name;

        private final Map<String, TestNode> children;

        private final boolean isDirectory;

        TestNode(String name, boolean isDirectory) {
            this.name = name;
            this.children = new HashMap<>();
            this.isDirectory = isDirectory;
        }

        TestNode(String name, TestNode... children) {
            this.name = name;
            this.children = new HashMap<>();
            for (TestNode child : children) {
                this.children.put(child.name, child);
            }
            this.isDirectory = true;
        }

        @Override
        public boolean isDirectory() {
            return isDirectory;
        }

        @Override
        public Collection<String> childNames() {
            return children.keySet();
        }

        @Override
        public MetadataNode child(String name) {
            return children.get(name);
        }
    }

    static {
        DATA = new MetadataShellState();
        DATA.setRoot(new TestNode("",
            new TestNode("alpha",
                new TestNode("beta",
                    new TestNode("gamma")
                ),
                new TestNode("theta")
            ),
            new TestNode("foo",
                new TestNode("a"),
                new TestNode("beta")
            ),
            new TestNode("zeta",
                new TestNode("c", false)
            ),
            new TestNode("zzz")
        ));
        DATA.setWorkingDirectory("foo");
    }

    static class InfoConsumer implements Consumer<Optional<MetadataNodeInfo>> {
        private Optional<List<MetadataNodeInfo>> infos = null;

        @Override
        public void accept(Optional<MetadataNodeInfo> info) {
            if (infos == null) {
                if (info.isPresent()) {
                    infos = Optional.of(new ArrayList<>());
                    infos.get().add(info.get());
                } else {
                    infos = Optional.empty();
                }
            } else {
                if (info.isPresent()) {
                    infos.get().add(info.get());
                } else {
                    throw new RuntimeException("Saw non-empty info after seeing empty info");
                }
            }
        }
    }

    @Test
    public void testStarGlob() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("*", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.of(Arrays.asList(
            new MetadataNodeInfo(new String[] {"foo", "a"},
                DATA.root().child("foo").child("a")),
            new MetadataNodeInfo(new String[] {"foo", "beta"},
                DATA.root().child("foo").child("beta")))), consumer.infos);
    }

    @Test
    public void testDotDot() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("..", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.of(Arrays.asList(
            new MetadataNodeInfo(new String[0], DATA.root()))), consumer.infos);
    }

    @Test
    public void testDoubleDotDot() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("../..", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.of(Arrays.asList(
            new MetadataNodeInfo(new String[0], DATA.root()))), consumer.infos);
    }

    @Test
    public void testZGlob() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("../z*", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.of(Arrays.asList(
            new MetadataNodeInfo(new String[] {"zeta"},
                DATA.root().child("zeta")),
            new MetadataNodeInfo(new String[] {"zzz"},
                DATA.root().child("zzz")))), consumer.infos);
    }

    @Test
    public void testBetaOrThetaGlob() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("../*/{beta,theta}", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.of(Arrays.asList(
            new MetadataNodeInfo(new String[] {"alpha", "beta"},
                DATA.root().child("alpha").child("beta")),
            new MetadataNodeInfo(new String[] {"alpha", "theta"},
                DATA.root().child("alpha").child("theta")),
            new MetadataNodeInfo(new String[] {"foo", "beta"},
                DATA.root().child("foo").child("beta")))), consumer.infos);
    }

    @Test
    public void testNotFoundGlob() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("epsilon", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.empty(), consumer.infos);
    }

    @Test
    public void testAbsoluteGlob() {
        InfoConsumer consumer = new InfoConsumer();
        GlobVisitor visitor = new GlobVisitor("/a?pha", consumer);
        visitor.accept(DATA);
        assertEquals(Optional.of(Arrays.asList(
            new MetadataNodeInfo(new String[] {"alpha"},
                DATA.root().child("alpha")))), consumer.infos);
    }
}
