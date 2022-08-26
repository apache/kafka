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

package org.apache.kafka.shell;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.shell.GlobVisitor.MetadataNodeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Timeout(value = 120000, unit = MILLISECONDS)
public class GlobVisitorTest {
    static private final MetadataNodeManager.Data DATA;

    static {
        DATA = new MetadataNodeManager.Data();
        DATA.root().mkdirs("alpha", "beta", "gamma");
        DATA.root().mkdirs("alpha", "theta");
        DATA.root().mkdirs("foo", "a");
        DATA.root().mkdirs("foo", "beta");
        DATA.root().mkdirs("zeta").create("c");
        DATA.root().mkdirs("zeta");
        DATA.root().create("zzz");
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
                DATA.root().directory("foo").child("a")),
            new MetadataNodeInfo(new String[] {"foo", "beta"},
                DATA.root().directory("foo").child("beta")))), consumer.infos);
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
                DATA.root().directory("alpha").child("beta")),
            new MetadataNodeInfo(new String[] {"alpha", "theta"},
                DATA.root().directory("alpha").child("theta")),
            new MetadataNodeInfo(new String[] {"foo", "beta"},
                DATA.root().directory("foo").child("beta")))), consumer.infos);
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
                DATA.root().directory("alpha")))), consumer.infos);
    }
}
