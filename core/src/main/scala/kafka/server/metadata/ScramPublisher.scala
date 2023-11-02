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

package kafka.server.metadata

import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.fault.FaultHandler


class ScramPublisher(
  conf: KafkaConfig,
  faultHandler: FaultHandler,
  nodeType: String,
  credentialProvider: CredentialProvider,
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  logIdent = s"[${name()}] "

  override def name(): String = s"ScramPublisher $nodeType id=${conf.nodeId}"

  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    onMetadataUpdate(delta, newImage)
  }

  def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
  ): Unit = {
    val deltaName = s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    try {
      // Apply changes to SCRAM credentials.
      Option(delta.scramDelta()).foreach { scramDelta =>
        scramDelta.changes().forEach {
          case (mechanism, userChanges) =>
            userChanges.forEach {
              case (userName, change) =>
                if (change.isPresent) {
                  credentialProvider.updateCredential(mechanism, userName, change.get().toCredential(mechanism))
                } else {
                  credentialProvider.removeCredentials(mechanism, userName)
                }
            }
        }
      }
    } catch {
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing SCRAM changes from $deltaName", t)
    }
  }
}
