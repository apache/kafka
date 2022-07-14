/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import kafka.utils.TestUtils
import org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV3
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class FeatureCommandUnitTest {

  @Test
  def testDescribeFeatureByReleaseVersion(): Unit = {
    val describeOutput = TestUtils.grabConsoleOutput(FeatureCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "describe", "--release", IBP_3_3_IV3.toString)))
    assertTrue(describeOutput.contains("Feature: metadata.version\tSupportedMinVersion: 1\tSupportedMaxVersion: 7"))
  }

  @Test
  def testSpecifyBothFeatureAndRelease(): Unit = {
    assertEquals(
      "Only one of --release or --feature may be specified with describe sub-command.\n",
      TestUtils.grabConsoleError(FeatureCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "describe", "--release", IBP_3_3_IV3.toString, "--feature", "metadata.version")))
    )

    assertEquals(
      "Cannot specify both --release and --feature with upgrade sub-command.\n",
      TestUtils.grabConsoleError(FeatureCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "upgrade", "--release", IBP_3_3_IV3.toString, "--feature", "metadata.version", "--version", "10")))
    )
  }

  @Test
  def testBadReleaseVersion(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "describe", "--release", "3-IV100"))
    )

    assertThrows(
      classOf[IllegalArgumentException],
      () => FeatureCommand.mainNoExit(Array("--bootstrap-server", "localhost:9092", "upgrade", "--release", "3-IV100"))
    )
  }
}
