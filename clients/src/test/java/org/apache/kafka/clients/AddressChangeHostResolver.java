/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import java.net.InetAddress;

class AddressChangeHostResolver implements HostResolver {
    private boolean useNewAddresses;
    private InetAddress[] initialAddresses;
    private InetAddress[] newAddresses;
    private int resolutionCount = 0;

    public AddressChangeHostResolver(InetAddress[] initialAddresses, InetAddress[] newAddresses) {
        this.initialAddresses = initialAddresses;
        this.newAddresses = newAddresses;
    }

    @Override
    public InetAddress[] resolve(String host) {
        ++resolutionCount;
        return useNewAddresses ? newAddresses : initialAddresses;
    }

    public void changeAddresses() {
        useNewAddresses = true;
    }

    public boolean useNewAddresses() {
        return useNewAddresses;
    }

    public int resolutionCount() {
        return resolutionCount;
    }
}
