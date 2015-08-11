/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import joptsimple.{OptionSet, OptionParser}
import kafka.cluster.RackLocator
import org.I0Itec.zkclient.ZkClient

import scala.collection.Map

trait RackLocatorCommandOptions {
  val parser = new OptionParser
  val rackLocator = parser.accepts("rack-locator-class",
    "the fully qualified class name which extends kafka.cluster.RackLocator and provides broker rack mapping")
    .withRequiredArg
    .describedAs("rack locator class name")
    .ofType(classOf[String])
  val rackLocatorProps = parser.accepts("rack-locator-properties",
    "rack locator properties as a list of comma delimited key=value pairs")
    .withRequiredArg()
    .describedAs("prop1=value1,prop2=value2")
    .ofType(classOf[String])
  val options: OptionSet

  def getBrokerRackMap(zkClient: ZkClient): Map[Int, String] = {
    if (options.has(rackLocator)) {
      val className = options.valueOf(rackLocator)
      val props = if (options.has(rackLocatorProps)) options.valueOf(rackLocatorProps)
                  else ""
      RackLocator.create(zkClient, className, props).getRackInfo()
    } else {
      Map()
    }
  }
}
