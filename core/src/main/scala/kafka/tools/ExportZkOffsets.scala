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

import java.io.FileWriter
import joptsimple._
import kafka.utils.{Logging, ZkUtils, ZKGroupTopicDirs, CommandLineUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.security.JaasUtils


/**
 *  A utility that retrieve the offset of broker partitions in ZK and
 *  prints to an output file in the following format:
 *  
 *  /consumers/group1/offsets/topic1/1-0:286894308
 *  /consumers/group1/offsets/topic1/2-0:284803985
 *  
 *  This utility expects 3 arguments:
 *  1. Zk host:port string
 *  2. group name (all groups implied if omitted)
 *  3. output filename
 *     
 *  To print debug message, add the following line to log4j.properties:
 *  log4j.logger.kafka.tools.ExportZkOffsets$=DEBUG
 *  (for eclipse debugging, copy log4j.properties to the binary directory in "core" such as core/bin)
 */
object ExportZkOffsets extends Logging {

  def main(args: Array[String]) {
    val parser = new OptionParser

    val zkConnectOpt = parser.accepts("zkconnect", "ZooKeeper connect string.")
                            .withRequiredArg()
                            .defaultsTo("localhost:2181")
                            .ofType(classOf[String])
    val groupOpt = parser.accepts("group", "Consumer group.")
                            .withRequiredArg()
                            .ofType(classOf[String])
    val outFileOpt = parser.accepts("output-file", "Output file")
                            .withRequiredArg()
                            .ofType(classOf[String])
    parser.accepts("help", "Print this message.")
    
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Export consumer offsets to an output file.")
            
    val options = parser.parse(args : _*)
    
    if (options.has("help")) {
       parser.printHelpOn(System.out)
       System.exit(0)
    }
    
    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, outFileOpt)
    
    val zkConnect  = options.valueOf(zkConnectOpt)
    val groups     = options.valuesOf(groupOpt)
    val outfile    = options.valueOf(outFileOpt)

    var zkUtils   : ZkUtils    = null
    val fileWriter : FileWriter  = new FileWriter(outfile)
    
    try {
      zkUtils = ZkUtils(zkConnect,
                        30000,
                        30000,
                        JaasUtils.isZkSecurityEnabled())
      
      var consumerGroups: Seq[String] = null

      if (groups.size == 0) {
        consumerGroups = zkUtils.getChildren(ZkUtils.ConsumersPath).toList
      }
      else {
        import scala.collection.JavaConversions._
        consumerGroups = groups
      }
      
      for (consumerGrp <- consumerGroups) {
        val topicsList = getTopicsList(zkUtils, consumerGrp)
        
        for (topic <- topicsList) {
          val bidPidList = getBrokeridPartition(zkUtils, consumerGrp, topic)
          
          for (bidPid <- bidPidList) {
            val zkGrpTpDir = new ZKGroupTopicDirs(consumerGrp,topic)
            val offsetPath = zkGrpTpDir.consumerOffsetDir + "/" + bidPid
            zkUtils.readDataMaybeNull(offsetPath)._1 match {
              case Some(offsetVal) =>
                fileWriter.write(offsetPath + ":" + offsetVal + "\n")
                debug(offsetPath + " => " + offsetVal)
              case None =>
                error("Could not retrieve offset value from " + offsetPath)
            }
          }
        }
      }      
    }
    finally {      
      fileWriter.flush()
      fileWriter.close()
    }
  }

  private def getBrokeridPartition(zkUtils: ZkUtils, consumerGroup: String, topic: String): List[String] =
    zkUtils.getChildrenParentMayNotExist("/consumers/%s/offsets/%s".format(consumerGroup, topic)).toList
  
  private def getTopicsList(zkUtils: ZkUtils, consumerGroup: String): List[String] =
    zkUtils.getChildren("/consumers/%s/offsets".format(consumerGroup)).toList

}
