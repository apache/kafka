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

import scala.annotation.implicitNotFound

/**
  * This is a trick to prevent the compiler from inferring the `Nothing` type in cases where it would be a bug to do
  * so. An example is the following method:
  *
  * ```
  * def body[T <: AbstractRequest](implicit classTag: ClassTag[T], nn: NotNothing[T]): T
  * ```
  *
  * If we remove the `nn` parameter and we invoke it without any type parameters (e.g. `request.body`), `Nothing` would
  * be inferred, which is not desirable. As defined above, we get a helpful compiler error asking the user to provide
  * the type parameter explicitly.
  */
@implicitNotFound("Unable to infer type parameter, please provide it explicitly.")
trait NotNothing[T]

object NotNothing {
  private val evidence: NotNothing[Any] = new Object with NotNothing[Any]

  implicit def notNothingEvidence[T](implicit n: T =:= T): NotNothing[T] = evidence.asInstanceOf[NotNothing[T]]
}
