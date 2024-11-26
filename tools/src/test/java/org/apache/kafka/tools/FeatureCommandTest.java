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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.Features;
import org.apache.kafka.server.common.MetadataVersion;

import net.sourceforge.argparse4j.inf.Namespace;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
public class FeatureCommandTest {

    private final List<Features> testingFeatures = Arrays.stream(Features.FEATURES).collect(Collectors.toList());

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testDescribeWithKRaft(ClusterInstance cluster) {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(), "describe"))
        );

        List<String> features = Arrays.stream(commandOutput.split("\n")).sorted().collect(Collectors.toList());

        // Change expected message to reflect latest MetadataVersion (SupportedMaxVersion increases when adding a new version)
        assertEquals("Feature: eligible.leader.replicas.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 1\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(0)));
        assertEquals("Feature: group.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 1\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(1)));
        assertEquals("Feature: kraft.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 1\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(2)));
        assertEquals("Feature: metadata.version\tSupportedMinVersion: 3.0-IV1\t" +
                "SupportedMaxVersion: 4.0-IV3\tFinalizedVersionLevel: 3.3-IV1\t", outputWithoutEpoch(features.get(3)));
        assertEquals("Feature: transaction.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 2\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(4)));
    }

    // Use the first MetadataVersion that supports KIP-919
    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_7_IV0)
    public void testDescribeWithKRaftAndBootstrapControllers(ClusterInstance cluster) {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-controller", cluster.bootstrapControllers(), "describe"))
        );

        List<String> features = Arrays.stream(commandOutput.split("\n")).sorted().collect(Collectors.toList());

        // Change expected message to reflect latest MetadataVersion (SupportedMaxVersion increases when adding a new version)
        assertEquals("Feature: eligible.leader.replicas.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 1\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(0)));
        assertEquals("Feature: group.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 1\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(1)));
        assertEquals("Feature: kraft.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 1\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(2)));
        assertEquals("Feature: metadata.version\tSupportedMinVersion: 3.0-IV1\t" +
                "SupportedMaxVersion: 4.0-IV3\tFinalizedVersionLevel: 3.7-IV0\t", outputWithoutEpoch(features.get(3)));
        assertEquals("Feature: transaction.version\tSupportedMinVersion: 0\t" +
                "SupportedMaxVersion: 2\tFinalizedVersionLevel: 0\t", outputWithoutEpoch(features.get(4)));
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testUpgradeMetadataVersionWithKraft(ClusterInstance cluster) {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--feature", "metadata.version=5"))
        );
        assertEquals("metadata.version was upgraded to 5.", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--metadata", "3.3-IV2"))
        );
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nmetadata.version was upgraded to 6."), commandOutput);
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_3_IV1)
    public void testDowngradeMetadataVersionWithKRaft(ClusterInstance cluster) {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "disable", "--feature", "metadata.version"))
        );
        // Change expected message to reflect possible MetadataVersion range 1-N (N increases when adding a new version)
        assertEquals("Could not disable metadata.version. The update failed for all features since the following " +
                "feature had an error: Invalid update version 0 for feature metadata.version. Local controller 3000 only supports versions 1-25", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--metadata", "3.3-IV0"))

        );
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nCould not downgrade metadata.version to 4." +
                " The update failed for all features since the following feature had an error: Invalid metadata.version 4." +
                " Refusing to perform the requested downgrade because it might delete metadata information."), commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--unsafe", "--metadata", "3.3-IV0"))

        );
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nCould not downgrade metadata.version to 4." +
                " The update failed for all features since the following feature had an error: Invalid metadata.version 4." +
                " Unsafe metadata downgrade is not supported in this version."), commandOutput);
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_8_IV0)
    public void testUpgradeWithReleaseVersion(ClusterInstance cluster) {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--release-version", "3.7-IV3"))

        );
        assertEquals("Could not upgrade metadata.version to 18. The update failed for all features since the following feature had an error:" +
             " Invalid update version 18 for feature metadata.version. Can't downgrade the version of this feature without setting the upgrade type to either safe or unsafe downgrade.", commandOutput);

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "upgrade", "--release-version", "3.9-IV0"))
        );

        assertEquals("kraft.version was upgraded to 1.\n" +
                "metadata.version was upgraded to 21.", commandOutput);
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_8_IV0)
    public void testDowngradeWithReleaseVersion(ClusterInstance cluster) {
        String commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(1, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--release-version", "3.9-IV0"))

        );
        assertTrue(commandOutput.contains("The update failed for all features since the following feature had an error:" +
                " Invalid update version 1 for feature kraft.version. Can't downgrade to a newer version."));
        assertTrue(commandOutput.contains("Could not downgrade group.version to 0."));
        assertTrue(commandOutput.contains("Could not downgrade transaction.version to 0."));
        assertTrue(commandOutput.contains("Could not downgrade kraft.version to 1."));
        assertTrue(commandOutput.contains("Could not downgrade metadata.version to 21."));

        commandOutput = ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, FeatureCommand.mainNoExit("--bootstrap-server", cluster.bootstrapServers(),
                        "downgrade", "--release-version", "3.7-IV3"))

        );
        assertEquals("eligible.leader.replicas.version was downgraded to 0.\n" +
                "group.version was downgraded to 0.\n" +
                "kraft.version was downgraded to 0.\n" +
                "metadata.version was downgraded to 18.\n" +
                "transaction.version was downgraded to 0.", commandOutput);
    }

    private String outputWithoutEpoch(String output) {
        int pos = output.indexOf("Epoch: ");
        return (pos > 0) ? output.substring(0, pos) : output;
    }

    /**
     * Unit test of {@link FeatureCommand} tool.
     */

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
    public void testDowngradeType() {
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
            assertTrue(t.getMessage().contains("2 out of 2 operation(s) failed."));
        });
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nCould not upgrade foo.bar to 6." +
            " Invalid update version 5 for feature metadata.version. Can't upgrade to lower version.%n" +
            "Could not upgrade metadata.version to 5. Invalid update version 5 for feature metadata.version. Can't upgrade to lower version."), upgradeOutput);
    }

    @Test
    public void testHandleUpgradeDryRun() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV1");
        namespace.put("feature", Collections.singletonList("foo.bar=6"));
        namespace.put("dry_run", true);
        String upgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleUpgrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("2 out of 2 operation(s) failed."));
        });
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nCan not upgrade foo.bar to 6." +
            " Invalid update version 5 for feature metadata.version. Can't upgrade to lower version.%n" +
            "Can not upgrade metadata.version to 5. Invalid update version 5 for feature metadata.version. Can't upgrade to lower version."), upgradeOutput);
    }

    @Test
    public void testHandleDowngrade() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV3");
        namespace.put("feature", Collections.singletonList("foo.bar=1"));
        namespace.put("dry_run", false);
        String downgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDowngrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("2 out of 2 operation(s) failed."));
        });
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nCould not downgrade foo.bar to 1." +
            " Invalid update version 7 for feature metadata.version. Can't downgrade to newer version.%n" +
            "Could not downgrade metadata.version to 7. Invalid update version 7 for feature metadata.version. Can't downgrade to newer version."), downgradeOutput);
    }

    @Test
    public void testHandleDowngradeDryRun() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("metadata", "3.3-IV3");
        namespace.put("feature", Collections.singletonList("foo.bar=1"));
        namespace.put("dry_run", true);
        String downgradeOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDowngrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("2 out of 2 operation(s) failed."));
        });
        assertEquals(format("`metadata` flag is deprecated and may be removed in a future release.%nCan not downgrade foo.bar to 1. Invalid update version 7 for feature metadata.version." +
            " Can't downgrade to newer version.%nCan not downgrade metadata.version to 7. Invalid update version 7 for feature metadata.version. Can't downgrade to newer version."), downgradeOutput);
    }

    @Test
    public void testHandleDisable() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Arrays.asList("foo.bar", "metadata.version", "quux"));
        namespace.put("dry_run", false);
        String disableOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDisable(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("3 out of 3 operation(s) failed."));
        });
        assertEquals(format("Could not disable foo.bar. Invalid update version 0 for feature metadata.version. Can't downgrade below 4%n" +
            "Could not disable metadata.version. Invalid update version 0 for feature metadata.version. Can't downgrade below 4%n" +
            "Could not disable quux. Invalid update version 0 for feature metadata.version. Can't downgrade below 4"), disableOutput);
    }

    @Test
    public void testHandleDisableDryRun() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Arrays.asList("foo.bar", "metadata.version", "quux"));
        namespace.put("dry_run", true);
        String disableOutput = ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleDisable(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("3 out of 3 operation(s) failed."));
        });
        assertEquals(format("Can not disable foo.bar. Invalid update version 0 for feature metadata.version. Can't downgrade below 4%n" +
            "Can not disable metadata.version. Invalid update version 0 for feature metadata.version. Can't downgrade below 4%n" +
            "Can not disable quux. Invalid update version 0 for feature metadata.version. Can't downgrade below 4"), disableOutput);
    }

    @Test
    public void testInvalidReleaseVersion() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("release_version", "foo");
        ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleUpgrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("Unknown metadata.version foo."));
        });
    }

    @Test
    public void testIncompatibleUpgradeFlags() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("release_version", "3.3-IV3");
        namespace.put("feature", Arrays.asList("foo.bar", "metadata.version", "quux"));
        ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleUpgrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("Can not specify `release-version` with other feature flags."));
        });


        namespace.put("release_version", "3.3-IV3");
        namespace.put("metadata", "3.3-IV3");
        ToolsTestUtils.captureStandardOut(() -> {
            Throwable t = assertThrows(TerseException.class, () -> FeatureCommand.handleUpgrade(new Namespace(namespace), buildAdminClient()));
            assertTrue(t.getMessage().contains("Can not specify `release-version` with other feature flags."));
        });
    }

    @Test
    public void testHandleVersionMappingWithValidReleaseVersion() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("release_version", "3.3-IV3");
        String versionMappingOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                FeatureCommand.handleVersionMapping(new Namespace(namespace), testingFeatures);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        MetadataVersion metadataVersion = MetadataVersion.IBP_3_3_IV3;

        // Check that the metadata version is correctly included in the output
        assertTrue(versionMappingOutput.contains("metadata.version=" + metadataVersion.featureLevel() + " (" + metadataVersion.version() + ")"),
            "Output did not contain expected Metadata Version: " + versionMappingOutput);

        for (Features feature : Features.values()) {
            int featureLevel = feature.defaultValue(metadataVersion);
            assertTrue(versionMappingOutput.contains(feature.featureName() + "=" + featureLevel),
                "Output did not contain expected feature mapping: " + versionMappingOutput);
        }
    }

    @Test
    public void testHandleVersionMappingWithNoReleaseVersion() {
        Map<String, Object> namespace = new HashMap<>();
        String versionMappingOutput = ToolsTestUtils.captureStandardOut(() -> {
            try {
                FeatureCommand.handleVersionMapping(new Namespace(namespace), testingFeatures);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        MetadataVersion metadataVersion = MetadataVersion.latestProduction();

        // Check that the metadata version is correctly included in the output
        assertTrue(versionMappingOutput.contains("metadata.version=" + metadataVersion.featureLevel() + " (" + metadataVersion.version() + ")"),
            "Output did not contain expected Metadata Version: " + versionMappingOutput);

        for (Features feature : Features.values()) {
            int featureLevel = feature.defaultValue(metadataVersion);
            assertTrue(versionMappingOutput.contains(feature.featureName() + "=" + featureLevel),
                "Output did not contain expected feature mapping: " + versionMappingOutput);
        }
    }

    @Test
    public void testHandleVersionMappingWithInvalidReleaseVersion() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("release_version", "2.9-IV2");

        TerseException exception1 = assertThrows(TerseException.class, () ->
            FeatureCommand.handleVersionMapping(new Namespace(namespace), testingFeatures)
        );

        assertEquals("Unknown release version '2.9-IV2'." +
            " Supported versions are: " + MetadataVersion.MINIMUM_BOOTSTRAP_VERSION +
            " to " + MetadataVersion.LATEST_PRODUCTION, exception1.getMessage());

        namespace.put("release_version", "invalid");

        TerseException exception2 = assertThrows(TerseException.class, () ->
            FeatureCommand.handleVersionMapping(new Namespace(namespace), testingFeatures)
        );

        assertEquals("Unknown release version 'invalid'." +
            " Supported versions are: " + MetadataVersion.MINIMUM_BOOTSTRAP_VERSION +
            " to " + MetadataVersion.LATEST_PRODUCTION, exception2.getMessage());
    }

    @Test
    public void testHandleFeatureDependenciesForFeatureWithDependencies() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Collections.singletonList("test.feature.version=2"));

        String output = ToolsTestUtils.captureStandardOut(() -> {
            try {
                FeatureCommand.handleFeatureDependencies(new Namespace(namespace), testingFeatures);
            } catch (TerseException e) {
                throw new RuntimeException(e);
            }
        });

        String expectedOutput = String.format(
            "test.feature.version=2 requires:\n    metadata.version=%d (%s)\n",
            MetadataVersion.latestTesting().featureLevel(),
            MetadataVersion.latestTesting().version()
        );

        assertEquals(expectedOutput.trim(), output.trim());
    }

    @Test
    public void testHandleFeatureDependenciesForFeatureWithNoDependencies() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Collections.singletonList("metadata.version=17"));

        String output = ToolsTestUtils.captureStandardOut(() -> {
            try {
                FeatureCommand.handleFeatureDependencies(new Namespace(namespace), testingFeatures);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals("metadata.version=17 (3.7-IV2) has no dependencies.", output);
    }

    @Test
    public void testHandleFeatureDependenciesForUnknownFeature() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Collections.singletonList("unknown.feature=1"));

        Exception exception = assertThrows(
            TerseException.class,
            () -> FeatureCommand.handleFeatureDependencies(new Namespace(namespace), testingFeatures)
        );

        assertEquals("Unknown feature: unknown.feature", exception.getMessage());
    }

    @Test
    public void testHandleFeatureDependenciesForFeatureWithUnknownFeatureVersion() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Collections.singletonList("transaction.version=1000"));

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> FeatureCommand.handleFeatureDependencies(new Namespace(namespace), testingFeatures)
        );

        assertEquals("No feature:transaction.version with feature level 1000", exception.getMessage());
    }

    @Test
    public void testHandleFeatureDependenciesForInvalidVersionFormat() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Collections.singletonList("metadata.version=invalid"));

        RuntimeException exception = assertThrows(
            RuntimeException.class,
            () -> FeatureCommand.handleFeatureDependencies(new Namespace(namespace), testingFeatures)
        );

        assertEquals(
            "Can't parse feature=level string metadata.version=invalid: unable to parse invalid as a short.",
            exception.getMessage()
        );
    }

    @Test
    public void testHandleFeatureDependenciesForMultipleFeatures() {
        Map<String, Object> namespace = new HashMap<>();
        namespace.put("feature", Arrays.asList(
                "transaction.version=2",
                "group.version=1",
                "test.feature.version=2"
        ));

        String output = ToolsTestUtils.captureStandardOut(() -> {
            try {
                FeatureCommand.handleFeatureDependencies(new Namespace(namespace), testingFeatures);
            } catch (TerseException e) {
                throw new RuntimeException(e);
            }
        });

        // Expected output for test.feature.version=2 dependencies
        String latestTestingVersionOutput = String.format(
                "test.feature.version=2 requires:\n    metadata.version=%d (%s)\n",
                MetadataVersion.latestTesting().featureLevel(),
                MetadataVersion.latestTesting().version()
        );

        String expectedOutput = String.join("\n",
                "transaction.version=2 has no dependencies.",
                "group.version=1 has no dependencies.",
                latestTestingVersionOutput.trim()
        );

        assertEquals(expectedOutput.trim(), output.trim());
    }
}
