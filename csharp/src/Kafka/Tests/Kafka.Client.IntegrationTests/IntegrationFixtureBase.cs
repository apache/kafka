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

namespace Kafka.Client.IntegrationTests
{
    using System;
    using System.Threading;
    using Kafka.Client.ZooKeeperIntegration;
    using NUnit.Framework;

    public abstract class IntegrationFixtureBase
    {
        protected string CurrentTestTopic { get; set; }

        [SetUp]
        public void SetupCurrentTestTopic()
        {
            CurrentTestTopic = TestContext.CurrentContext.Test.Name + "_" + Guid.NewGuid().ToString();
        }

        internal static void WaitUntillIdle(IZooKeeperClient client, int timeout)
        {
            Thread.Sleep(timeout);
            int rest = timeout - client.IdleTime;
            while (rest > 0)
            {
                Thread.Sleep(rest);
                rest = timeout - client.IdleTime;
            }
        }
    }
}