# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# TODO(ksweeney): RAM requirements are not empirical and can probably be significantly lowered.
Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "precise64"

  # The url from where the 'config.vm.box' box will be fetched if it
  # doesn't already exist on the user's system.
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"

  config.vm.define "zookeeper" do |zookeeper|
    zookeeper.vm.network :private_network, ip: "192.168.50.5"
    zookeeper.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    zookeeper.vm.provision "shell", path: "vagrant/zk.sh"
  end

  config.vm.define "brokerOne" do |brokerOne|
    brokerOne.vm.network :private_network, ip: "192.168.50.10"
    brokerOne.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    brokerOne.vm.provision "shell", path: "vagrant/broker.sh", :args => "1"
  end

  config.vm.define "brokerTwo" do |brokerTwo|
    brokerTwo.vm.network :private_network, ip: "192.168.50.20"
    brokerTwo.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    brokerTwo.vm.provision "shell", path: "vagrant/broker.sh", :args => "2"
  end

  config.vm.define "brokerThree" do |brokerThree|
    brokerThree.vm.network :private_network, ip: "192.168.50.30"
    brokerThree.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    brokerThree.vm.provision "shell", path: "vagrant/broker.sh", :args => "3"
  end  

end
