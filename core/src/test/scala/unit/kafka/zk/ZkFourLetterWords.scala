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

package kafka.zk

import java.io.IOException
import java.net.{SocketTimeoutException, Socket, InetAddress, InetSocketAddress}

/**
  * ZooKeeper responds to a small set of commands. Each command is composed of four letters. You issue the commands to
  * ZooKeeper via telnet or nc, at the client port.
  *
  * Three of the more interesting commands: "stat" gives some general information about the server and connected
  * clients, while "srvr" and "cons" give extended details on server and connections respectively.
  */
object ZkFourLetterWords {
  def sendStat(host: String, port: Int, timeout: Int): Unit = {
    val hostAddress =
      if (host != null) new InetSocketAddress(host, port)
      else new InetSocketAddress(InetAddress.getByName(null), port)
    val sock = new Socket()
    try {
      sock.connect(hostAddress, timeout)
      val outStream = sock.getOutputStream
      outStream.write("stat".getBytes)
      outStream.flush()
    } catch {
      case e: SocketTimeoutException => throw new IOException("Exception while sending 4lw", e)
    } finally {
      sock.close
    }
  }
}
