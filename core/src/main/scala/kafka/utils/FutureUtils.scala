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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

/**
  * Created by aozeritsky on 08.05.2017.
  */
object FutureUtils {
  // GC friendly Future.sequence
  def sequence[T](futures: Traversable[Future[T]])(implicit executor: ExecutionContext) = {
    val promise = Promise[Seq[T]]()
    val aggregator = new ConcurrentLinkedQueue[T]
    val expectedSize = futures.size

    for (future <- futures) {
      future onComplete {
        case Success(value) => {
          aggregator.add(value)

          if (aggregator.size == expectedSize) {
            promise.trySuccess(aggregator.asScala.toBuffer)
          }
        }

        case Failure(reason) =>
          promise.tryFailure(reason)
      }
    }

    promise.future
  }
}
