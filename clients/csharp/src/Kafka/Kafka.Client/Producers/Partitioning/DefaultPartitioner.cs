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
    using System;
    using Kafka.Client.Utils;

    /// <summary>
    /// Default partitioner using hash code to calculate partition
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    public class DefaultPartitioner<TKey> : IPartitioner<TKey>
        where TKey : class 
    {
        private static readonly Random Randomizer = new Random();

        /// <summary>
        /// Uses the key to calculate a partition bucket id for routing
        /// the data to the appropriate broker partition
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="numPartitions">The num partitions.</param>
        /// <returns>ID between 0 and numPartitions-1</returns>
        /// <remarks>
        /// Used hash code to calculate partition
        /// </remarks>
        public int Partition(TKey key, int numPartitions)
        {
            Guard.Assert<ArgumentOutOfRangeException>(() => numPartitions > 0);
            return key == null 
                ? Randomizer.Next(numPartitions) 
                : key.GetHashCode() % numPartitions;
        }
    }
}
