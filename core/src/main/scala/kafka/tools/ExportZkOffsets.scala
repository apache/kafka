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
import kafka.utils.{Logging, ZkUtils, ZKStringSerializer,ZKGroupTopicDirs}
import org.I0Itec.zkclient.ZkClient


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
            
    val options = parser.parse(args : _*)
    
    if (options.has("help")) {
       parser.printHelpOn(System.out)
       System.exit(0)
    }
    
    for (opt <- List(zkConnectOpt, outFileOpt)) {
      if (!options.has(opt)) {
        System.err.println("Missing required argument: %s".format(opt))
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val zkConnect  = options.valueOf(zkConnectOpt)
    val groups     = options.valuesOf(groupOpt)
    val outfile    = options.valueOf(outFileOpt)

    var zkClient   : ZkClient    = null
    val fileWriter : FileWriter  = new FileWriter(outfile)
    
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      
      var consumerGroups: Seq[String] = null

      if (groups.size == 0) {
        consumerGroups = ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath).toList
      }
      else {
        import scala.collection.JavaConversions._
        consumerGroups = groups
      }
      
      for (consumerGrp <- consumerGroups) {
        val topicsList = getTopicsList(zkClient, consumerGrp)
        
        for (topic <- topicsList) {
          val bidPidList = getBrokeridPartition(zkClient, consumerGrp, topic)
          
          for (bidPid <- bidPidList) {
            val zkGrpTpDir = new ZKGroupTopicDirs(consumerGrp,topic)
            val offsetPath = zkGrpTpDir.consumerOffsetDir + "/" + bidPid
            val offsetVal  = ZkUtils.readDataMaybeNull(zkClient, offsetPath)
            fileWriter.write(offsetPath + ":" + offsetVal + "\n")
            debug(offsetPath + " => " + offsetVal)
          }
        }
      }      
    }
    finally {      
      fileWriter.flush()
      fileWriter.close()
    }
  }

  private def getBrokeridPartition(zkClient: ZkClient, consumerGroup: String, topic: String): List[String] = {
    return ZkUtils.getChildrenParentMayNotExist(zkClient, "/consumers/%s/offsets/%s".format(consumerGroup, topic)).toList
  }
  
  private def getTopicsList(zkClient: ZkClient, consumerGroup: String): List[String] = {
    return ZkUtils.getChildren(zkClient, "/consumers/%s/offsets".format(consumerGroup)).toList
  }
}
