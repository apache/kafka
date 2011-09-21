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

namespace Kafka.Client.Utils
{
    using System;
    using System.Globalization;
    using System.Reflection;
    using System.Threading;
    using log4net;

    /// <summary>
    /// A scheduler for running jobs in the background
    /// </summary>
    internal class KafkaScheduler : IDisposable
    {
        public delegate void KafkaSchedulerDelegate();

        private Timer timer;

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private KafkaSchedulerDelegate methodToRun;

        private volatile bool disposed;

        private readonly object shuttingDownLock = new object();

        public void ScheduleWithRate(KafkaSchedulerDelegate method, long delayMs, long periodMs)
        {
            methodToRun = method;
            TimerCallback tcb = HandleCallback;
            timer = new Timer(tcb, null, delayMs, periodMs);
        }

        private void HandleCallback(object o)
        {
            methodToRun();
        }

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            lock (this.shuttingDownLock)
            {
                if (this.disposed)
                {
                    return;
                }

                this.disposed = true;
            }

            try
            {
                if (timer != null)
                {
                    timer.Dispose();
                    Logger.InfoFormat(CultureInfo.CurrentCulture, "shutdown scheduler");
                }
            }
            catch (Exception exc)
            {
                Logger.Warn("Ignoring unexpected errors on closing", exc);
            }
        }
    }
}