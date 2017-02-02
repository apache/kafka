/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import kafka.utils.Exit
import org.apache.zookeeper.ZooKeeperMain

class ZooKeeperMainWrapper(args: Array[String]) extends ZooKeeperMain(args) {
  def runCmd(): Unit = {
    processCmd(this.cl)
    Exit.exit(0)
  }
}

/**
 * ZooKeeper 3.4.6 broke being able to pass commands on command line.
 * See ZOOKEEPER-1897.  This class is a hack to restore this facility.
 */
object ZooKeeperMainWrapper {

  def main(args: Array[String]): Unit = {
    val main: ZooKeeperMainWrapper = new ZooKeeperMainWrapper(args)
    main.runCmd()
  }
}
