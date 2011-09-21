using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Producers.Partitioning;

namespace Kafka.Client.IntegrationTests
{
    /// <summary>
    /// This mock partitioner will always point to the first partition (the one of index = 0)
    /// </summary>
    public class MockAlwaysZeroPartitioner : IPartitioner<string>
    {
        public int Partition(string key, int numPartitions)
        {
            return 0;
        }
    }
}
