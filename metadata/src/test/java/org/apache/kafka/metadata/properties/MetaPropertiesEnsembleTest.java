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

package org.apache.kafka.metadata.properties;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.EMPTY;
import static org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.META_PROPERTIES_NAME;
import static org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.REQUIRE_AT_LEAST_ONE_VALID;
import static org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.REQUIRE_METADATA_LOG_DIR;
import static org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.REQUIRE_V0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class MetaPropertiesEnsembleTest {
    private static final MetaPropertiesEnsemble FOO =
        new MetaPropertiesEnsemble(
            new HashSet<>(Arrays.asList("/tmp/empty1", "/tmp/empty2")),
            new HashSet<>(Arrays.asList("/tmp/error3")),
            Arrays.asList(
                new SimpleImmutableEntry<>("/tmp/dir4",
                    new MetaProperties.Builder().
                        setVersion(MetaPropertiesVersion.V1).
                        setClusterId("fooClusterId").
                        setNodeId(2).
                        build()),
                new SimpleImmutableEntry<>("/tmp/dir5",
                    new MetaProperties.Builder().
                        setVersion(MetaPropertiesVersion.V1).
                        setClusterId("fooClusterId").
                        setNodeId(2).
                        build())).stream().collect(Collectors.
                            toMap(Entry::getKey, Entry::getValue)),
                Optional.of("/tmp/dir4"));

    private static String createLogDir(MetaProperties metaProps) throws IOException {
        File logDir = TestUtils.tempDirectory();
        PropertiesUtils.writePropertiesFile(metaProps.toProperties(),
            new File(logDir, META_PROPERTIES_NAME).getAbsolutePath(), false);
        return logDir.getAbsolutePath();
    }

    private static String createEmptyLogDir() throws IOException {
        File logDir = TestUtils.tempDirectory();
        return logDir.getAbsolutePath();
    }

    private static String createErrorLogDir() throws IOException {
        File logDir = TestUtils.tempDirectory();
        File metaPath = new File(logDir, META_PROPERTIES_NAME);
        Files.write(metaPath.toPath(), new byte[] {(byte) 0});
        metaPath.setReadable(false);
        return logDir.getAbsolutePath();
    }

    @Test
    public void testEmptyLogDirsForFoo() {
        assertEquals(new HashSet<>(Arrays.asList("/tmp/empty1", "/tmp/empty2")),
            FOO.emptyLogDirs());
    }

    @Test
    public void testEmptyLogDirsForEmpty() {
        assertEquals(new HashSet<>(), EMPTY.emptyLogDirs());
    }

    @Test
    public void testErrorLogDirsForFoo() {
        assertEquals(new HashSet<>(Arrays.asList("/tmp/error3")), FOO.errorLogDirs());
    }

    @Test
    public void testErrorLogDirsForEmpty() {
        assertEquals(new HashSet<>(), EMPTY.errorLogDirs());
    }

    @Test
    public void testLogDirPropsForFoo() {
        assertEquals(new HashSet<>(Arrays.asList("/tmp/dir4", "/tmp/dir5")),
            FOO.logDirProps().keySet());
    }

    @Test
    public void testLogDirPropsForEmpty() {
        assertEquals(new HashSet<>(),
            EMPTY.logDirProps().keySet());
    }

    @Test
    public void testNonFailedDirectoryPropsForFoo() {
        Map<String, Optional<MetaProperties>> results = new HashMap<>();
        FOO.nonFailedDirectoryProps().forEachRemaining(entry -> {
            results.put(entry.getKey(), entry.getValue());
        });
        assertEquals(Optional.empty(), results.get("/tmp/empty1"));
        assertEquals(Optional.empty(), results.get("/tmp/empty2"));
        assertNull(results.get("/tmp/error3"));
        assertEquals(Optional.of(new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("fooClusterId").
            setNodeId(2).
            build()), results.get("/tmp/dir4"));
        assertEquals(Optional.of(new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("fooClusterId").
            setNodeId(2).
            build()), results.get("/tmp/dir5"));
        assertEquals(4, results.size());
    }

    @Test
    public void testNonFailedDirectoryPropsForEmpty() {
        assertFalse(EMPTY.nonFailedDirectoryProps().hasNext());
    }

    @Test
    public void testMetadataLogDirForFoo() {
        assertEquals(Optional.of("/tmp/dir4"), FOO.metadataLogDir());
    }

    @Test
    public void testMetadataLogDirForEmpty() {
        assertEquals(Optional.empty(), EMPTY.metadataLogDir());
    }

    @Test
    public void testNodeIdForFoo() {
        assertEquals(OptionalInt.of(2), FOO.nodeId());
    }

    @Test
    public void testNodeIdForEmpty() {
        assertEquals(OptionalInt.empty(), EMPTY.nodeId());
    }

    @Test
    public void testClusterIdForFoo() {
        assertEquals(Optional.of("fooClusterId"), FOO.clusterId());
    }

    @Test
    public void testClusterIdForEmpty() {
        assertEquals(Optional.empty(), EMPTY.clusterId());
    }

    @Test
    public void testSuccessfulVerification() {
        FOO.verify(Optional.empty(),
            OptionalInt.empty(),
            EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR));
    }

    @Test
    public void testSuccessfulVerificationWithClusterId() {
        FOO.verify(Optional.of("fooClusterId"),
            OptionalInt.empty(),
            EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR));
    }

    @Test
    public void testSuccessfulVerificationWithClusterIdAndNodeId() {
        FOO.verify(Optional.of("fooClusterId"),
            OptionalInt.of(2),
            EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR));
    }

    @Test
    public void testVerificationFailureOnRequireV0() {
        assertEquals("Found unexpected version in /tmp/dir4/meta.properties. ZK-based brokers " +
            "that are not migrating only support version 0 (which is implicit when the " +
            "`version` field is missing).",
                assertThrows(RuntimeException.class, () ->
                    FOO.verify(Optional.empty(), OptionalInt.empty(), EnumSet.of(REQUIRE_V0))).
                        getMessage());
    }

    @Test
    public void testVerificationFailureOnRequireAtLeastOneValid() {
        assertEquals("No readable meta.properties files found.",
            assertThrows(RuntimeException.class,
                () -> EMPTY.verify(Optional.empty(),
                    OptionalInt.empty(),
                    EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID))).
                        getMessage());
    }

    @Test
    public void testVerificationFailureOnLackOfMetadataLogDir() throws IOException {
        MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble(
            Collections.singleton("/tmp/foo1"),
            Collections.emptySet(),
            Collections.emptyMap(),
            Optional.empty());
        assertEquals("No metadata log directory was specified.",
            assertThrows(RuntimeException.class,
                () -> ensemble.verify(Optional.empty(),
                    OptionalInt.empty(),
                    EnumSet.of(REQUIRE_METADATA_LOG_DIR))).
                        getMessage());
    }

    @Test
    public void testVerificationFailureOnMetadataLogDirWithError() throws IOException {
        MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble(
            Collections.emptySet(),
            Collections.singleton("/tmp/foo1"),
            Collections.emptyMap(),
            Optional.of("/tmp/foo1"));
        assertEquals("Encountered I/O error in metadata log directory /tmp/foo1. Cannot continue.",
            assertThrows(RuntimeException.class,
                () -> ensemble.verify(Optional.empty(),
                    OptionalInt.empty(),
                    EnumSet.of(REQUIRE_METADATA_LOG_DIR))).
                        getMessage());
    }

    @Test
    public void testMetaPropertiesEnsembleLoad() throws IOException {
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        MetaProperties metaProps = new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("AtgGav8yQjiaJ3rTXE7VCA").
            setNodeId(1).
            build();
        loader.addMetadataLogDir(createLogDir(metaProps));
        MetaPropertiesEnsemble metaPropertiesEnsemble = loader.load();
        metaPropertiesEnsemble.verify(Optional.of("AtgGav8yQjiaJ3rTXE7VCA"),
            OptionalInt.of(1),
            EnumSet.of(REQUIRE_METADATA_LOG_DIR, REQUIRE_AT_LEAST_ONE_VALID));
        assertEquals(1, metaPropertiesEnsemble.logDirProps().values().size());
        assertEquals(metaProps, metaPropertiesEnsemble.logDirProps().values().iterator().next());
    }

    @Test
    public void testMetaPropertiesEnsembleLoadEmpty() throws IOException {
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        loader.addMetadataLogDir(createEmptyLogDir());
        MetaPropertiesEnsemble metaPropertiesEnsemble = loader.load();
        metaPropertiesEnsemble.verify(Optional.of("AtgGav8yQjiaJ3rTXE7VCA"),
            OptionalInt.of(1),
            EnumSet.of(REQUIRE_METADATA_LOG_DIR));
        assertEquals(1, metaPropertiesEnsemble.emptyLogDirs().size());
    }

    @Test
    public void testMetaPropertiesEnsembleLoadError() throws IOException {
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        loader.addMetadataLogDir(createErrorLogDir());
        loader.addLogDir(createLogDir(new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("AtgGav8yQjiaJ3rTXE7VCA").
            setNodeId(1).
            build()));
        MetaPropertiesEnsemble metaPropertiesEnsemble = loader.load();
        assertEquals(1, metaPropertiesEnsemble.errorLogDirs().size());
        assertEquals(1, metaPropertiesEnsemble.logDirProps().size());
    }

    static private void verifyCopy(
        MetaPropertiesEnsemble expected,
        MetaPropertiesEnsemble.Copier copier
    ) {
        copier.verify();
        MetaPropertiesEnsemble foo2 = copier.copy();
        assertEquals(expected, foo2);
        assertEquals(expected.hashCode(), foo2.hashCode());
        assertEquals(expected.toString(), foo2.toString());
    }

    @Test
    public void testCopierWithoutModifications() {
        verifyCopy(FOO, new MetaPropertiesEnsemble.Copier(FOO));
    }

    @Test
    public void testCopyFooItemByItem() {
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(EMPTY);
        copier.setMetaLogDir(FOO.metadataLogDir());
        FOO.emptyLogDirs().forEach(e -> copier.emptyLogDirs().add(e));
        FOO.logDirProps().entrySet().
            forEach(e -> copier.logDirProps().put(e.getKey(), e.getValue()));
        FOO.errorLogDirs().forEach(e -> copier.errorLogDirs().add(e));
        verifyCopy(FOO, copier);
    }

    static class MetaPropertiesMockRandom extends Random {
        private final AtomicInteger index = new AtomicInteger(0);

        private List<Long> results = Arrays.asList(
            0L,
            0L,
            2336837413447398698L,
            1758400403264101670L,
            4341931186263415792L,
            6389410885970711333L,
            7265008559332826740L,
            3478747443029687715L
        );

        @Override
        public long nextLong() {
            int curIndex = index.getAndIncrement();
            return results.get(curIndex % results.size());
        }
    }

    @Test
    public void testCopierGenerateValidDirectoryId() {
        MetaPropertiesMockRandom random = new MetaPropertiesMockRandom();
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(EMPTY);
        copier.setRandom(random);
        copier.logDirProps().put("/tmp/dir1",
            new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId("PpYMbsoRQV-589isZzNzEw").
                setNodeId(0).
                setDirectoryId(new Uuid(2336837413447398698L, 1758400403264101670L)).
                build());
        copier.logDirProps().put("/tmp/dir2",
            new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId("PpYMbsoRQV-589isZzNzEw").
                setNodeId(0).
                setDirectoryId(new Uuid(4341931186263415792L, 6389410885970711333L)).
                build());
        // Verify that we ignore the non-safe IDs, or the IDs that have already been used,
        // when invoking generateValidDirectoryId.
        assertEquals(new Uuid(7265008559332826740L, 3478747443029687715L),
            copier.generateValidDirectoryId());
    }

    @Test
    public void testCopierVerificationFailsOnEmptyAndErrorOverlap() {
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(EMPTY);
        copier.emptyLogDirs().add("/tmp/foo");
        copier.errorLogDirs().add("/tmp/foo");
        assertEquals("Error: log directory /tmp/foo is in both emptyLogDirs and errorLogDirs.",
            assertThrows(RuntimeException.class, () -> copier.verify()).getMessage());
    }

    @Test
    public void testCopierVerificationFailsOnEmptyAndLogDirsOverlap() {
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(EMPTY);
        copier.emptyLogDirs().add("/tmp/foo");
        copier.logDirProps().put("/tmp/foo", new MetaProperties.Builder().build());
        assertEquals("Error: log directory /tmp/foo is in both emptyLogDirs and logDirProps.",
            assertThrows(RuntimeException.class, () -> copier.verify()).getMessage());
    }

    @Test
    public void testCopierVerificationFailsOnErrorAndLogDirsOverlap() {
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(EMPTY);
        copier.errorLogDirs().add("/tmp/foo");
        copier.logDirProps().put("/tmp/foo", new MetaProperties.Builder().build());
        assertEquals("Error: log directory /tmp/foo is in both errorLogDirs and logDirProps.",
            assertThrows(RuntimeException.class, () -> copier.verify()).getMessage());
    }

    private final static List<MetaProperties> SAMPLE_META_PROPS_LIST = Arrays.asList(
        new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("AtgGav8yQjiaJ3rTXE7VCA").
            setNodeId(1).
            setDirectoryId(Uuid.fromString("s33AdXtkR8Gf_xRO-R_dpA")).
            build(),
        new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("AtgGav8yQjiaJ3rTXE7VCA").
            setNodeId(1).
            setDirectoryId(Uuid.fromString("oTM53yT_SbSfzlvkh_PfVA")).
            build(),
        new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId("AtgGav8yQjiaJ3rTXE7VCA").
            setNodeId(1).
            setDirectoryId(Uuid.fromString("FcUhIv2mTzmLqGkVEabyag")).
            build());

    @Test
    public void testCopierWriteLogDirChanges() throws Exception {
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        loader.addMetadataLogDir(createLogDir(SAMPLE_META_PROPS_LIST.get(0)));
        MetaPropertiesEnsemble ensemble = loader.load();
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(ensemble);
        String newLogDir1 = createEmptyLogDir();
        copier.logDirProps().put(newLogDir1, SAMPLE_META_PROPS_LIST.get(1));
        String newLogDir2 = createEmptyLogDir();
        copier.logDirProps().put(newLogDir2, SAMPLE_META_PROPS_LIST.get(2));
        copier.writeLogDirChanges();
        assertEquals(SAMPLE_META_PROPS_LIST.get(1).toProperties(), PropertiesUtils.readPropertiesFile(
            new File(newLogDir1, META_PROPERTIES_NAME).getAbsolutePath()));
        assertEquals(SAMPLE_META_PROPS_LIST.get(2).toProperties(), PropertiesUtils.readPropertiesFile(
            new File(newLogDir2, META_PROPERTIES_NAME).getAbsolutePath()));
    }

    @Test
    public void testCopierWriteChanged() throws Exception {
        MetaPropertiesEnsemble.Loader loader = new MetaPropertiesEnsemble.Loader();
        String dir0 = createLogDir(SAMPLE_META_PROPS_LIST.get(0));
        loader.addMetadataLogDir(dir0);
        loader.addLogDir(dir0);
        String dir1 = createLogDir(SAMPLE_META_PROPS_LIST.get(1));
        loader.addLogDir(dir1);
        MetaPropertiesEnsemble ensemble = loader.load();
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(ensemble);
        copier.setLogDirProps(dir0, SAMPLE_META_PROPS_LIST.get(2));
        copier.writeLogDirChanges();
        assertEquals(SAMPLE_META_PROPS_LIST.get(2).toProperties(), PropertiesUtils.readPropertiesFile(
                new File(dir0, META_PROPERTIES_NAME).getAbsolutePath()));
        assertEquals(SAMPLE_META_PROPS_LIST.get(1).toProperties(), PropertiesUtils.readPropertiesFile(
                new File(dir1, META_PROPERTIES_NAME).getAbsolutePath()));
    }
}
