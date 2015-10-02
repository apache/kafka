/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

/*
 * We inherit from `Product` and `Serializable` because `case` objects and classes inherit from them and if we don't
 * do it here, the compiler will infer types that unexpectedly include `Product` and `Serializable`, see
 * http://underscore.io/blog/posts/2015/06/04/more-on-sealed.html for more information.
 */
trait BaseEnum extends Product with Serializable {
  def name: String
}
