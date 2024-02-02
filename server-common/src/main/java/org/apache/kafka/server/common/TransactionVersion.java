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
package org.apache.kafka.server.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.kafka.common.record.RecordVersion;

/**
 * *
 */
public enum TransactionVersion {

    // No transaction features are supported. We chose 3.3 so any bootstrap version sets the TV to 0.
    TV_0(0, "3.3"),

    // Use flexible version for transaction state records.
    TV_1(1, "3.8"),

    // Enable transaction protocol V2.
    TV_2(2, "3.8");

    // NOTES when adding a new version:
    //   Update the default version in @ClusterTest annotation to point to the latest version
    //   Change expected message in org.apache.kafka.tools.FeatureCommandTest in multiple places (search for "Change expected message")
    public static final String FEATURE_NAME = "transaction.version";

    // Minimum version is the version we use when no transaction features are supported.
    public static final TransactionVersion MINIMUM_VERSION = TV_0;

    /**
     * The latest production-ready TransactionVersion. This is the latest version that is stable
     * and cannot be changed. TransactionVersions later than this can be tested via junit, but
     * not deployed in production.
     *
     * <strong>Think carefully before you update this value. ONCE A TRANSACTION VERSION IS PRODUCTION,
     * IT CANNOT BE CHANGED.</strong>
     */
    public static final TransactionVersion LATEST_PRODUCTION = TV_1;

    /**
     * An array containing all the TransactionVersion entries.
     *
     * This is essentially a cached copy of TransactionVersion.values. Unlike that function, it doesn't
     * allocate a new array each time.
     */
    public static final TransactionVersion[] VERSIONS;

    private final short featureLevel;
    private final String release;

    TransactionVersion(int featureLevel, String release) {
        this.featureLevel = (short) featureLevel;
        this.release = release;
    }

    public short featureLevel() {
        return featureLevel;
    }

    public boolean isTransactionsV2Enabled() {
        return this.isAtLeast(TV_2);
    }

    public RecordVersion highestSupportedRecordVersion() {
        if (this.isLessThan(TV_1)) {
            return RecordVersion.V0;
        } else {
            return RecordVersion.V1;
        }
    }



    private static final Map<String, TransactionVersion> RELEASE_TRANSACTIONS_VERSIONS;

    static {
        TransactionVersion[] enumValues = TransactionVersion.values();
        VERSIONS = Arrays.copyOf(enumValues, enumValues.length);

        RELEASE_TRANSACTIONS_VERSIONS = new HashMap<>();
        Map<String, TransactionVersion> maxReleaseVersion = new HashMap<>();
        for (TransactionVersion transactionVersion : VERSIONS) {
            if (transactionVersion.isProduction()) {
                maxReleaseVersion.put(transactionVersion.release, transactionVersion);
            }
        }
        RELEASE_TRANSACTIONS_VERSIONS.putAll(maxReleaseVersion);
    }

    public String shortVersion() {
        return release;
    }

    private static boolean versionLessThan(String[] thisVersion, String[] thatVersion) {
        // We can take the shortest version array since any versions of different lengths should be decided on the major version
        int maxVersionLength = Integer.max(thisVersion.length, thatVersion.length);

        for (int i = 0; i < maxVersionLength; i++) {
            if (Integer.parseInt(thisVersion[i]) < Integer.parseInt(thatVersion[i])) {
                return true;
            }
        }
        return false;
    }


    /**
     * Return an `TransactionVersion` instance for `versionString`, which should be passed in the form of a
     * kafka release. Some examples are 3.3, 3.6, 4.0. Patch releases (3.5.1) wil be ignored.
     */
    public static TransactionVersion fromVersionString(String versionString) {
        String[] versionSegments = versionString.split(Pattern.quote("."));
        int numSegments = 2;
        String key;
        if (numSegments >= versionSegments.length) {
            key = versionString;
        } else {
            key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
        }


        return Optional.ofNullable(RELEASE_TRANSACTIONS_VERSIONS.get(key)).orElseGet(() -> {
            TransactionVersion highestSupportedVersion = null;
            try {
                for (int i = 0; i < VERSIONS.length && versionLessThan(VERSIONS[i].release.split(Pattern.quote(".")), versionSegments); i++) {
                    highestSupportedVersion = VERSIONS[i];
                }
            } catch (NumberFormatException e) {
                // break the loop. We will throw an error if there was no valid version.
            }
            return Optional.ofNullable(highestSupportedVersion).orElseThrow(() ->
                new IllegalArgumentException("Version " + versionString + " is not a valid version")
            );
        });
    }

    public static TransactionVersion fromFeatureLevel(short version) {
        for (TransactionVersion transactionVersion: TransactionVersion.values()) {
            if (transactionVersion.featureLevel() == version) {
                return transactionVersion;
            }
        }
        throw new IllegalArgumentException("No TransactionVersion with transaction version " + version);
    }

    // Testing only
    public static TransactionVersion latestTesting() {
        return VERSIONS[VERSIONS.length - 1];
    }

    public static TransactionVersion latestProduction() {
        return LATEST_PRODUCTION;
    }

    public boolean isProduction() {
        return this.compareTo(TransactionVersion.LATEST_PRODUCTION) <= 0;
    }

    public boolean isAtLeast(TransactionVersion otherVersion) {
        return this.compareTo(otherVersion) >= 0;
    }

    public boolean isLessThan(TransactionVersion otherVersion) {
        return this.compareTo(otherVersion) < 0;
    }
}
