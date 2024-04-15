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
package org.apache.kafka.tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import static org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType.SAFE_DOWNGRADE;
import static org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.KRAFT)
@Tag("integration")
public class FeatureCommandTest {

    private final ClusterInstance cluster;
    public FeatureCommandTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testDescribeWithZK() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe"))
        );
        assertEquals("", commandOutput);
    }

    @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testDescribeWithKRaft() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe"))
        );
        // Change expected message to reflect latest MetadataVersion (SupportedMaxVersion increases when adding a new version)
        assertEquals("Feature: metadata.version\tSupportedMinVersion: 3.0-IV1\t" +
                "SupportedMaxVersion: 3.8-IV0\tFinalizedVersionLevel: 3.3-IV1\t", outputWithoutEpoch(commandOutput));
    }

    @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_7_IV4)
    public void testDescribeWithKRaftAndBootstrapControllers() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-controller", cluster.bootstrapControllers(), "describe"))
        );
        // Change expected message to reflect latest MetadataVersion (SupportedMaxVersion increases when adding a new version)
        assertEquals("Feature: metadata.version\tSupportedMinVersion: 3.0-IV1\t" +
                "SupportedMaxVersion: 3.8-IV0\tFinalizedVersionLevel: 3.7-IV4\t", outputWithoutEpoch(commandOutput));
    }

    @ClusterTest(clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testUpgradeMetadataVersionWithZk() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--metadata", "3.3-IV2"))
        );
        assertEquals("Could not upgrade metadata.version to 6. Could not apply finalized feature " +
                "update because the provided feature is not supported.", commandOutput);
    }

    @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testUpgradeMetadataVersionWithKraft() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--feature", "metadata.version=5"))
        );
        assertEquals("metadata.version was upgraded to 5.", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--metadata", "3.3-IV2"))
        );
        assertEquals("metadata.version was upgraded to 6.", commandOutput);
    }

    @ClusterTest(clusterType = Type.ZK, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testDowngradeMetadataVersionWithZk() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "disable", "--feature", "metadata.version"))
        );
        assertEquals("Could not disable metadata.version. Can not delete non-existing finalized feature.", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--metadata", "3.3-IV0"))
        );
        assertEquals("Could not downgrade metadata.version to 4. Could not apply finalized feature " +
                        "update because the provided feature is not supported.", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--unsafe", "--metadata", "3.3-IV0"))
        );
        assertEquals("Could not downgrade metadata.version to 4. Could not apply finalized feature " +
                "update because the provided feature is not supported.", commandOutput);
    }

    @ClusterTest(clusterType = Type.KRAFT, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testDowngradeMetadataVersionWithKRaft() {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "disable", "--feature", "metadata.version"))
        );
        // Change expected message to reflect possible MetadataVersion range 1-N (N increases when adding a new version)
        assertEquals("Could not disable metadata.version. Invalid update version 0 for feature " +
                "metadata.version. Local controller 3000 only supports versions 1-20", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--metadata", "3.3-IV0"))

        );
        assertEquals("Could not downgrade metadata.version to 4. Invalid metadata.version 4. " +
                "Refusing to perform the requested downgrade because it might delete metadata information.", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--unsafe", "--metadata", "3.3-IV0"))

        );
        assertEquals("Could not downgrade metadata.version to 4. Invalid metadata.version 4. " +
                "Unsafe metadata downgrade is not supported in this version.", commandOutput);
    }

    private String outputWithoutEpoch(String output) {
        int pos = output.indexOf("Epoch: ");
        return (pos > 0) ? output.substring(0, pos) : output;
    }
}

class FeatureCommandUnitTest {
    @Test
    public void testLevelToString() {
        assertEquals("5", FeatureCommand.levelToString("foo.bar", (short) 5));
        assertEquals("3.3-IV0",
                FeatureCommand.levelToString(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel()));
    }

    @Test
    public void testMetadataVersionsToString() {
        assertEquals("3.3-IV0, 3.3-IV1, 3.3-IV2, 3.3-IV3",
                FeatureCommand.metadataVersionsToString(MetadataVersion.IBP_3_3_IV0, MetadataVersion.IBP_3_3_IV3));
    }

    @Test
    public void testdowngradeType() {
        assertEquals(SAFE_DOWNGRADE, FeatureCommand.downgradeType(
                new Namespace(singletonMap("unsafe", Boolean.FALSE))));
        assertEquals(UNSAFE_DOWNGRADE, FeatureCommand.downgradeType(
                new Namespace(singletonMap("unsafe", Boolean.TRUE))));
        assertEquals(SAFE_DOWNGRADE, FeatureCommand.downgradeType(new Namespace(emptyMap())));
    }

    @Test
    public void testParseNameAndLevel() {
        assertArrayEquals(new String[]{"foo.bar", "5"}, FeatureCommand.parseNameAndLevel("foo.bar=5"));
        assertArrayEquals(new String[]{"quux", "0"}, FeatureCommand.parseNameAndLevel("quux=0"));
        assertTrue(assertThrows(RuntimeException.class, () -> FeatureCommand.parseNameAndLevel("baaz"))
                .getMessage().contains("Can't parse feature=level string baaz: equals sign not found."));
        assertTrue(assertThrows(RuntimeException.class, () -> FeatureCommand.parseNameAndLevel("w=tf"))
                .getMessage().contains("Can't parse feature=level string w=tf: unable to parse tf as a short."));
    }

    private static MockAdminClient buildAdminClient() {
        Map<String, Short> minSupportedFeatureLevels = new HashMap<>();
        minSupportedFeatureLevels.put(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV0.featureLevel());
        minSupportedFeatureLevels.put("foo.bar", (short) 0);

        Map<String, Short> featureLevels = new HashMap<>();
        featureLevels.put(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV2.featureLevel());
        featureLevels.put("foo.bar", (short) 5);

        Map<String, Short> maxSupportedFeatureLevels = new HashMap<>();
        maxSupportedFeatureLevels.put(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_3_IV3.featureLevel());
        maxSupportedFeatureLevels.put("foo.bar", (short) 10);

        return new MockAdminClient.Builder().
                minSupportedFeatureLevels(minSupportedFeatureLevels).
                featureLevels(featureLevels).
                maxSupportedFeatureLevels(maxSupportedFeatureLevels).build();
    }

    @Test
    public void testHandleDescribe() {
        String describeResult = ToolsTestUtils.captureStandardOut(() -> {
            try {
                FeatureCommand.handleDescribe(buildAdminClient());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertEquals(format("Feature: foo.bar\tSupportedMinVersion: 0\tSupportedMaxVersion: 10\tFinalizedVersionLevel: 5\tEpoch: 123%n" +
                "Feature: metadata.version\tSupportedMinVersion: 3.3-IV0\tSupportedMaxVersion: 3.3-IV3\tFinalizedVersionLevel: 3.3-IV2\tEpoch: 123"), describeResult);
    }

    @Test
    public void testHandleUpgrade() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV1");
        namespace.put("feature", Collections.singletonList("foo.bar=6"));
        namespace.put("dry_run", false);
        String upgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleUpgrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("1 out of 2 operation(s) failed."));
        });
        assertEquals(format("foo.bar was upgraded to 6.%n" +
                "Could not upgrade metadata.version to 5. Can't upgrade to lower version."), upgradeOutput);
    }

    @Test
    public void testHandleUpgradeDryRun() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV1");
        namespace.put("feature", Collections.singletonList("foo.bar=6"));
        namespace.put("dry_run", true);
        String upgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleUpgrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("1 out of 2 operation(s) failed."));
        });
        assertEquals(format("foo.bar can be upgraded to 6.%n" +
                "Can not upgrade metadata.version to 5. Can't upgrade to lower version."), upgradeOutput);
    }

    @Test
    public void testHandleDowngrade() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV3");
        namespace.put("feature", Collections.singletonList("foo.bar=1"));
        namespace.put("dry_run", false);
        String downgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDowngrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("1 out of 2 operation(s) failed."));
        });
        assertEquals(format("foo.bar was downgraded to 1.%n" +
                "Could not downgrade metadata.version to 7. Can't downgrade to newer version."), downgradeOutput);
    }

    @Test
    public void testHandleDowngradeDryRun() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV3");
        namespace.put("feature", Collections.singletonList("foo.bar=1"));
        namespace.put("dry_run", true);
        String downgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDowngrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("1 out of 2 operation(s) failed."));
        });
        assertEquals(format("foo.bar can be downgraded to 1.%n" +
                "Can not downgrade metadata.version to 7. Can't downgrade to newer version."), downgradeOutput);
    }

    @Test
    public void testHandleDisable() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Arrays.asList("foo.bar", "metadata.version", "quux"));
        namespace.put("dry_run", false);
        String disableOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDisable(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("1 out of 3 operation(s) failed."));
        });
        assertEquals(format("foo.bar was disabled.%n" +
                "Could not disable metadata.version. Can't downgrade below 4%n" +
                "quux was disabled."), disableOutput);
    }

    @Test
    public void testHandleDisableDryRun() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Arrays.asList("foo.bar", "metadata.version", "quux"));
        namespace.put("dry_run", true);
        String disableOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDisable(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("1 out of 3 operation(s) failed."));
        });
        assertEquals(format("foo.bar can be disabled.%n" +
                "Can not disable metadata.version. Can't downgrade below 4%n" +
                "quux can be disabled."), disableOutput);
    }
}
