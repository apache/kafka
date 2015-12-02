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

package kafka.tools

import java.io.BufferedReader
import java.io.FileReader
import joptsimple._
import kafka.utils.{Logging, ZkUtils, CommandLineUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.security.JaasUtils


/**
 *  A utility that updates the offset of broker partitions in ZK.
 *  
 *  This utility expects 2 input files as arguments:
 *  1. consumer properties file
 *  2. a file contains partition offsets data such as:
 *     (This output data file can be obtained by running kafka.tools.ExportZkOffsets)
 *  
 *     /consumers/group1/offsets/topic1/3-0:285038193
 *     /consumers/group1/offsets/topic1/1-0:286894308
 *     
 *  To print debug message, add the following line to log4j.properties:
 *  log4j.logger.kafka.tools.ImportZkOffsets$=DEBUG
 *  (for eclipse debugging, copy log4j.properties to the binary directory in "core" such as core/bin)
 */
object ImportZkOffsets extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser
    
    val zkConnectOpt = parser.accepts("zkconnect", "ZooKeeper connect string.")
                            .withRequiredArg()
                            .defaultsTo("localhost:2181")
                            .ofType(classOf[String])
    val inFileOpt = parser.accepts("input-file", "Input file")
                            .withRequiredArg()
                            .ofType(classOf[String])
    parser.accepts("help", "Print this message.")
    
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Import offsets to zookeeper from files.")
            
    val options = parser.parse(args : _*)
    
    if (options.has("help")) {
       parser.printHelpOn(System.out)
       System.exit(0)
    }
    
    CommandLineUtils.checkRequiredArgs(parser, options, inFileOpt)
    
    val zkConnect           = options.valueOf(zkConnectOpt)
    val partitionOffsetFile = options.valueOf(inFileOpt)

    val zkUtils = ZkUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled())
    val partitionOffsets: Map[String,String] = getPartitionOffsetsFromFile(partitionOffsetFile)

    updateZkOffsets(zkUtils, partitionOffsets)
  }

  private def getPartitionOffsetsFromFile(filename: String):Map[String,String] = {
    val fr = new FileReader(filename)
    val br = new BufferedReader(fr)
    var partOffsetsMap: Map[String,String] = Map()
    
    var s: String = br.readLine()
    while ( s != null && s.length() >= 1) {
      val tokens = s.split(":")
      
      partOffsetsMap += tokens(0) -> tokens(1)
      debug("adding node path [" + s + "]")
      
      s = br.readLine()
    }
    
    partOffsetsMap
  }
  
  private def updateZkOffsets(zkUtils: ZkUtils, partitionOffsets: Map[String,String]): Unit = {
    for ((partition, offset) <- partitionOffsets) {
      debug("updating [" + partition + "] with offset [" + offset + "]")
      
      try {
        zkUtils.updatePersistentPath(partition, offset.toString)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
  }
}
