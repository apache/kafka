namespace Kafka.Client.Exceptions
{
    using System;

    public class ZooKeeperTimeoutException : Exception
    {
        public ZooKeeperTimeoutException()
            : base("Unable to connect to zookeeper server within timeout: unknown value")
        {
        }

        public ZooKeeperTimeoutException(int connectionTimeout)
            : base("Unable to connect to zookeeper server within timeout: " + connectionTimeout)
        {
        }
    }
}
