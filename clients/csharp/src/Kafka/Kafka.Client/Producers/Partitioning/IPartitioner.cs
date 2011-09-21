/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

namespace Kafka.Client.Producers.Partitioning
{
    /// <summary>
    /// User-defined partitioner
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    public interface IPartitioner<TKey>
        where TKey : class 
    {
        /// <summary>
        /// Uses the key to calculate a partition bucket id for routing
        /// the data to the appropriate broker partition
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="numPartitions">The num partitions.</param>
        /// <returns>ID between 0 and numPartitions-1</returns>
        int Partition(TKey key, int numPartitions);
    }
}
