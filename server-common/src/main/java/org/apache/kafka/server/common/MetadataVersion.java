package org.apache.kafka.server.common;

//public interface SupportedVersions {
//  boolean isAlterIsrSupported();
//  boolean isAllocateProducerIdsSupported();
//  boolean isSettingLeaderRecoveryStateSupported();
//
//
//}

import static java.lang.Integer.min;
import static java.lang.String.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.RecordVersion;


public enum MetadataVersion {
  IBP_0_8_0(-1),
  IBP_0_8_1(-1),
  IBP_0_8_2(-1),
  IBP_0_9_0(-1),
  IBP_0_10_0_IV0(-1),
  IBP_0_10_0_IV1(-1),
  IBP_0_10_1_IV0(-1),
  IBP_0_10_1_IV1(-1),
  IBP_0_10_1_IV2(-1),
  IBP_0_10_2_IV0(-1),
  IBP_0_11_0_IV0(-1),
  IBP_0_11_0_IV1(-1),
  IBP_0_11_0_IV2(-1),
  IBP_1_0_IV0(-1),
  IBP_1_1_IV0(-1),
  IBP_2_0_IV0(-1),
  IBP_2_0_IV1(-1),
  IBP_2_1_IV0(-1),
  IBP_2_1_IV1(-1),
  IBP_2_1_IV2(-1),
  IBP_2_2_IV0(-1),
  IBP_2_2_IV1(-1),
  IBP_2_3_IV0(-1),
  IBP_2_3_IV1(-1),
  IBP_2_4_IV0(-1),
  IBP_2_4_IV1(-1),
  IBP_2_5_IV0(-1),
  IBP_2_6_IV0(-1),
  IBP_2_7_IV0(-1),
  IBP_2_7_IV1(-1),
  IBP_2_7_IV2(-1),
  IBP_2_8_IV0(-1),
  IBP_2_8_IV1(-1),
  // KRaft preview
  IBP_3_0_IV0(1),
  IBP_3_0_IV1(2),
  IBP_3_1_IV1(3),
  IBP_3_2_IV0(4),
  // KRaft GA
  IBP_3_3_IV0(5);

  private final Optional<Short> metadataVersion;

  MetadataVersion(int metadataVersion) {
    if (metadataVersion > 0) {
      this.metadataVersion = Optional.of((short) metadataVersion);
    } else {
      this.metadataVersion = Optional.empty();
    }
  }

  public boolean isAlterIsrSupported() {
    return this.compareTo(IBP_2_7_IV2) >= 0;
  }

  public boolean isAllocateProducerIdsSupported() {
    return this.compareTo(IBP_3_0_IV0) >= 0;
  }

  public boolean isTruncationOnFetchSupported() {
    return this.compareTo(IBP_2_7_IV1) >= 0;
  }

  public boolean isTopicIdsSupported() {
    return this.compareTo(IBP_2_8_IV0) >= 0;
  }

  public boolean isFeatureVersioningSupported() {
    return this.compareTo(IBP_2_7_IV1) >= 0;
  }

  public boolean isSaslInterBrokerHandshakeRequestEnabled() {
    return this.compareTo(IBP_0_10_0_IV1) >= 0;
  }

  public RecordVersion recordVersion() {
    if (this.compareTo(IBP_0_9_0) <= 0 ) { // IBPs up to IBP_0_9_0 use Record Version V0
      return RecordVersion.V0;
    } else if (this.compareTo(IBP_0_10_2_IV0) <= 0 ) { // IBPs up to IBP_0_10_2_IV0 use V1
      return RecordVersion.V1;
    } else return RecordVersion.V2; // all greater IBPs use V2
  }

  private static final Map<String, MetadataVersion> ibpVersions;
  static {{
    ibpVersions = new HashMap<>();
    Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)(?:IV(\\d))?");
    Map<String, MetadataVersion> maxInterVersion = new HashMap<>();
    for (MetadataVersion version : MetadataVersion.values()) {
      Matcher matcher = versionPattern.matcher(version.name());
      if (matcher.find()) {
        String withoutIV = matcher.group(1);
        // remove any trailing underscores
        if (withoutIV.endsWith("_")) {
          withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
        }
        String shortVersion = withoutIV.replace("_", ".");

        String normalizedVersion;
        if (matcher.group(2) != null) {
          normalizedVersion = String.format("%s-IV%s", shortVersion, matcher.group(2));
        } else {
          normalizedVersion = shortVersion;
        }
        maxInterVersion.compute(shortVersion, (__, currentVersion) -> {
          if (currentVersion == null) {
            return version;
          } else if (version.compareTo(currentVersion) > 0) {
            return version;
          } else {
            return currentVersion;
          }
        });
        ibpVersions.put(normalizedVersion, version);
      } else {
        throw new IllegalArgumentException("Metadata version: " + version.name() + " does not fit "
            + "any of the accepted patterns.");
      }
    }
    ibpVersions.putAll(maxInterVersion);
  }}

  public String shortVersion() {
    Pattern versionPattern = Pattern.compile("^IBP_([\\d_]+)");
    Matcher matcher = versionPattern.matcher(this.name());
    if (matcher.find()) {
      String withoutIV = matcher.group(1);
      // remove any trailing underscores
      if (withoutIV.endsWith("_")) {
        withoutIV = withoutIV.substring(0, withoutIV.length() - 1);
      }
      return withoutIV.replace("_", ".");
    } else {
      throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
          + "the accepted pattern.");
    }
  }

  public String version() {
    if (this.compareTo(IBP_0_10_0_IV0) < 0) { // versions less than this do not have IV versions
      return shortVersion();
    } else {
      Pattern ivPattern = Pattern.compile("^IBP_[\\d_]+IV(\\d)");
      Matcher matcher = ivPattern.matcher(this.name());
      if (matcher.find()) {
        return String.format("%s-%s", shortVersion(), matcher.group(1));
      } else {
        throw new IllegalArgumentException("Metadata version: " + this.name() + " does not fit "
            + "the accepted pattern.");
      }
    }
  }

  /**
   * Return an `ApiVersion` instance for `versionString`, which can be in a variety of formats (e.g. "0.8.0", "0.8.0.x",
   * "0.10.0", "0.10.0-IV1"). `IllegalArgumentException` is thrown if `versionString` cannot be mapped to an `ApiVersion`.
   */
  public static MetadataVersion apply(String versionString) {
    String[] versionSegments = versionString.split(Pattern.quote("."));
    int desiredSegments = (versionString.startsWith("0.")) ? 3 : 2;
    int numSegments = min(desiredSegments, versionSegments.length);
    String key;
    if (numSegments >= versionSegments.length) {
      key = versionString;
    } else {
      key = String.join(".", Arrays.copyOfRange(versionSegments, 0, numSegments));
    }
    return Optional.ofNullable(ibpVersions.get(key)).orElseThrow(() ->
        new IllegalArgumentException("Version " + versionString + "is not a valid version")
    );
  }

  /**
   * Return the minimum `MetadataVersion` that supports `RecordVersion`.
   */
  public static MetadataVersion minSupportedFor(RecordVersion recordVersion) {
    switch (recordVersion) {
      case V0:
        return IBP_0_8_0;
      case V1:
        return IBP_0_10_0_IV0;
      case V2:
        return IBP_0_11_0_IV0;
      default:
        throw new IllegalArgumentException("Invalid message format version " + recordVersion);
    }
  }

  public static MetadataVersion latest() {
    MetadataVersion[] values = MetadataVersion.values();
    return values[values.length - 1];
  }

  public static MetadataVersion stable() {
    return MetadataVersion.IBP_3_3_IV0;
  }

  public Optional<Short> metadataVersion() {
    return metadataVersion;
  }
}

public class MetadataVersionValidator implements Validator {

  @Override
  void ensureValid(String name, Object value) {
    try {
      new MetadataVersion.apply(value.toString()); // ahu todo: fix
    } catch (IllegalArgumentException e) {
      throw new ConfigException(name, value.toString(), e.getMessage());
    }
  }

  @Override
  public String toString() {
    return "[" + Arrays.stream(MetadataVersion.values()).distinct().map(m -> m.version()).collect(
        Collectors.joining(", ")) + "]";
  }
}

