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

package kafka

import org.apache.log4j.PropertyConfigurator
import kafka.utils.{Exit, Logging}
import serializer.Encoder

object TestKafkaAppender extends Logging {
  
  def main(args:Array[String]): Unit = {
    
    if(args.length < 1) {
      println("USAGE: " + TestKafkaAppender.getClass.getName + " log4j_config")
      Exit.exit(1)
    }

    try {
      PropertyConfigurator.configure(args(0))
    } catch {
      case e: Exception =>
        System.err.println("KafkaAppender could not be initialized ! Exiting..")
        e.printStackTrace()
        Exit.exit(1)
    }

    for (_ <- 1 to 10)
      info("test")    
  }
}

class AppenderStringSerializer(encoding: String = "UTF-8") extends Encoder[AnyRef] {
  def toBytes(event: AnyRef): Array[Byte] = event.toString.getBytes(encoding)
}

