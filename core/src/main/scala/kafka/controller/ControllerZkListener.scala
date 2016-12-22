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
package kafka.controller

import kafka.utils.Logging
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener}

import scala.collection.JavaConverters._

trait ControllerZkListener extends Logging {
  logIdent = s"[$logName on Controller " + controller.config.brokerId + "]: "
  protected def logName: String
  protected def controller: KafkaController
}

trait ControllerZkChildListener extends IZkChildListener with ControllerZkListener {
  @throws[Exception]
  final def handleChildChange(parentPath: String, currentChildren: java.util.List[String]): Unit = {
    // Due to zkclient's callback order, it's possible for the callback to be triggered after the controller has moved
    if (controller.isActive)
      doHandleChildChange(parentPath, currentChildren.asScala)
  }

  @throws[Exception]
  def doHandleChildChange(parentPath: String, currentChildren: Seq[String]): Unit
}

trait ControllerZkDataListener extends IZkDataListener with ControllerZkListener {
  @throws[Exception]
  final def handleDataChange(dataPath: String, data: AnyRef): Unit = {
    // Due to zkclient's callback order, it's possible for the callback to be triggered after the controller has moved
    if (controller.isActive)
      doHandleDataChange(dataPath, data)
  }

  @throws[Exception]
  def doHandleDataChange(dataPath: String, data: AnyRef): Unit

  @throws[Exception]
  final def handleDataDeleted(dataPath: String): Unit = {
    // Due to zkclient's callback order, it's possible for the callback to be triggered after the controller has moved
    if (controller.isActive)
      doHandleDataDeleted(dataPath)
  }

  @throws[Exception]
  def doHandleDataDeleted(dataPath: String): Unit
}
