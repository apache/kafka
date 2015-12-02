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
package kafka.utils


object JaasTestUtils {
  // ZooKeeper vals
  val zkServerContextName = "Server"
  val zkClientContextName = "Client"
  val userSuperPasswd = "adminpasswd"
  val user = "fpj"
  val userPasswd = "fpjsecret"
  val zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule"
  //Kafka vals
  val kafkaServerContextName = "KafkaServer"
  val kafkaClientContextName = "KafkaClient"
  val kafkaServerPrincipal = "client@EXAMPLE.COM"
  val kafkaClientPrincipal = "kafka/localhost@EXAMPLE.COM"
  val kafkaModule = "com.sun.security.auth.module.Krb5LoginModule"
  
  def genZkFile: String = {
    val jaasFile = java.io.File.createTempFile("jaas", ".conf")
    val jaasOutputStream = new java.io.FileOutputStream(jaasFile)
    writeZkToOutputStream(jaasOutputStream)
    jaasOutputStream.close()
    jaasFile.deleteOnExit()
    jaasFile.getCanonicalPath
  }
  
  def genKafkaFile(keytabLocation: String): String = {
    val jaasFile = java.io.File.createTempFile("jaas", ".conf")
    val jaasOutputStream = new java.io.FileOutputStream(jaasFile)
    writeKafkaToOutputStream(jaasOutputStream, keytabLocation)
    jaasOutputStream.close()
    jaasFile.deleteOnExit()
    jaasFile.getCanonicalPath
  }
  
  def genZkAndKafkaFile(keytabLocation: String): String = {
    val jaasFile = java.io.File.createTempFile("jaas", ".conf")
    val jaasOutputStream = new java.io.FileOutputStream(jaasFile)
    writeKafkaToOutputStream(jaasOutputStream, keytabLocation)
    jaasOutputStream.write("\n\n".getBytes)
    writeZkToOutputStream(jaasOutputStream)
    jaasOutputStream.close()
    jaasFile.deleteOnExit()
    jaasFile.getCanonicalPath
  }
  
  private def writeZkToOutputStream(jaasOutputStream: java.io.FileOutputStream) {
    jaasOutputStream.write(s"$zkServerContextName {\n\t$zkModule required\n".getBytes)
    jaasOutputStream.write(s"""\tuser_super="$userSuperPasswd"\n""".getBytes)
    jaasOutputStream.write(s"""\tuser_$user="$userPasswd";\n};\n\n""".getBytes)
    jaasOutputStream.write(s"""$zkClientContextName {\n\t$zkModule required\n""".getBytes)
    jaasOutputStream.write(s"""\tusername="$user"\n""".getBytes)
    jaasOutputStream.write(s"""\tpassword="$userPasswd";\n};""".getBytes)
  }
  
  private def writeKafkaToOutputStream(jaasOutputStream: java.io.FileOutputStream, keytabLocation: String) {
    jaasOutputStream.write(s"$kafkaClientContextName {\n\t$kafkaModule required debug=true\n".getBytes)
    jaasOutputStream.write(s"\tuseKeyTab=true\n".getBytes)
    jaasOutputStream.write(s"\tstoreKey=true\n".getBytes)
    jaasOutputStream.write(s"""\tserviceName="kafka"\n""".getBytes)
    jaasOutputStream.write(s"""\tkeyTab="$keytabLocation"\n""".getBytes)
    jaasOutputStream.write(s"""\tprincipal="$kafkaServerPrincipal";\n};\n\n""".getBytes)
    jaasOutputStream.write(s"""$kafkaServerContextName {\n\t$kafkaModule required debug=true\n""".getBytes)
    jaasOutputStream.write(s"\tuseKeyTab=true\n".getBytes)
    jaasOutputStream.write(s"\tstoreKey=true\n".getBytes)
    jaasOutputStream.write(s"""\tserviceName="kafka"\n""".getBytes)
    jaasOutputStream.write(s"""\tkeyTab="$keytabLocation"\n""".getBytes)
    jaasOutputStream.write(s"""\tprincipal="$kafkaClientPrincipal";\n};""".getBytes)
  }
}