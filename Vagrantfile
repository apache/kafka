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

require 'socket'

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# General config
enable_dns = false
# Override to false when bringing up a cluster on AWS
enable_hostmanager = true
enable_jmx = false
num_zookeepers = 1
num_brokers = 3
num_workers = 0 # Generic workers that get the code, but don't start any services
ram_megabytes = 1280
base_box = "ubuntu/trusty64"

# EC2
ec2_access_key = ENV['AWS_ACCESS_KEY']
ec2_secret_key = ENV['AWS_SECRET_KEY']
ec2_session_token = ENV['AWS_SESSION_TOKEN']
ec2_keypair_name = nil
ec2_keypair_file = nil

ec2_region = "us-east-1"
ec2_az = nil # Uses set by AWS
ec2_ami = "ami-29ebb519"
ec2_instance_type = "m3.medium"
ec2_spot_instance = ENV['SPOT_INSTANCE'] ? ENV['SPOT_INSTANCE'] == 'true' : true
ec2_spot_max_price = "0.113"  # On-demand price for instance type
ec2_user = "ubuntu"
ec2_instance_name_prefix = "kafka-vagrant"
ec2_security_groups = nil
ec2_subnet_id = nil
# Only override this by setting it to false if you're running in a VPC and you
# are running Vagrant from within that VPC as well.
ec2_associate_public_ip = nil
ec2_iam_instance_profile_name = nil

ebs_volume_type = 'gp3'

jdk_major = '17'
jdk_full = '17-linux-x64'

local_config_file = File.join(File.dirname(__FILE__), "Vagrantfile.local")
if File.exist?(local_config_file) then
  eval(File.read(local_config_file), binding, "Vagrantfile.local")
end

# override any instance type set by Vagrantfile.local or above via an environment variable
if ENV['INSTANCE_TYPE'] then
  ec2_instance_type = ENV['INSTANCE_TYPE']
end

# choose size based on overridden size
if ec2_instance_type.start_with?("m3") then
  ebs_volume_size = 20
else
  ebs_volume_size = 40
end

# TODO(ksweeney): RAM requirements are not empirical and can probably be significantly lowered.
Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.hostmanager.enabled = enable_hostmanager
  config.hostmanager.manage_host = enable_dns
  config.hostmanager.include_offline = false

  ## Provider-specific global configs
  config.vm.provider :virtualbox do |vb,override|
    override.vm.box = base_box

    override.hostmanager.ignore_private_ip = false

    # Brokers started with the standard script currently set Xms and Xmx to 1G,
    # plus we need some extra head room.
    vb.customize ["modifyvm", :id, "--memory", ram_megabytes.to_s]

    if Vagrant.has_plugin?("vagrant-cachier")
      override.cache.scope = :box
    end
  end

  config.vm.provider :aws do |aws,override|
    # The "box" is specified as an AMI
    override.vm.box = "dummy"
    override.vm.box_url = "https://github.com/mitchellh/vagrant-aws/raw/master/dummy.box"

    cached_addresses = {}
    # Use a custom resolver that SSH's into the machine and finds the IP address
    # directly. This lets us get at the private IP address directly, avoiding
    # some issues with using the default IP resolver, which uses the public IP
    # address.
    override.hostmanager.ip_resolver = proc do |vm, resolving_vm|
      if !cached_addresses.has_key?(vm.name)
        state_id = vm.state.id
        if state_id != :not_created && state_id != :stopped && vm.communicate.ready?
          contents = ''
          vm.communicate.execute("/sbin/ifconfig eth0 | grep 'inet addr' | tail -n 1 | egrep -o '[0-9\.]+' | head -n 1 2>&1") do |type, data|
            contents << data
          end
          cached_addresses[vm.name] = contents.split("\n").first[/(\d+\.\d+\.\d+\.\d+)/, 1]
        else
          cached_addresses[vm.name] = nil
        end
      end
      cached_addresses[vm.name]
    end

    override.ssh.username = ec2_user
    override.ssh.private_key_path = ec2_keypair_file

    aws.access_key_id = ec2_access_key
    aws.secret_access_key = ec2_secret_key
    aws.session_token = ec2_session_token
    aws.keypair_name = ec2_keypair_name

    aws.region = ec2_region
    aws.availability_zone = ec2_az
    aws.instance_type = ec2_instance_type

    aws.ami = ec2_ami
    aws.security_groups = ec2_security_groups
    aws.subnet_id = ec2_subnet_id
    aws.block_device_mapping = [{ 'DeviceName' => '/dev/sda1', 'Ebs.VolumeType' => ebs_volume_type, 'Ebs.VolumeSize' => ebs_volume_size }]
    # If a subnet is specified, default to turning on a public IP unless the
    # user explicitly specifies the option. Without a public IP, Vagrant won't
    # be able to SSH into the hosts unless Vagrant is also running in the VPC.
    if ec2_associate_public_ip.nil?
      aws.associate_public_ip = true unless ec2_subnet_id.nil?
    else
      aws.associate_public_ip = ec2_associate_public_ip
    end
    aws.region_config ec2_region do |region|
      region.spot_instance = ec2_spot_instance
      region.spot_max_price = ec2_spot_max_price
    end
    aws.iam_instance_profile_name = ec2_iam_instance_profile_name

    # Exclude some directories that can grow very large from syncing
    override.vm.synced_folder ".", "/vagrant", type: "rsync", rsync__exclude: ['.git', 'core/data/', 'logs/', 'tests/results/', 'results/']
  end

  def name_node(node, name, ec2_instance_name_prefix)
    node.vm.hostname = name
    node.vm.provider :aws do |aws|
      aws.tags = {
        'Name' => ec2_instance_name_prefix + "-" + Socket.gethostname + "-" + name,
        'JenkinsBuildUrl' => ENV['BUILD_URL']
      }
    end
  end

  def assign_local_ip(node, ip_address)
    node.vm.provider :virtualbox do |vb,override|
      override.vm.network :private_network, ip: ip_address
    end
  end

  ## Cluster definition
  zookeepers = []
  (1..num_zookeepers).each { |i|
    name = "zk" + i.to_s
    zookeepers.push(name)
    config.vm.define name do |zookeeper|
      name_node(zookeeper, name, ec2_instance_name_prefix)
      ip_address = "192.168.50." + (10 + i).to_s
      assign_local_ip(zookeeper, ip_address)
      zookeeper.vm.provision "shell", path: "vagrant/base.sh", env: {"JDK_MAJOR" => jdk_major, "JDK_FULL" => jdk_full}
      zk_jmx_port = enable_jmx ? (8000 + i).to_s : ""
      zookeeper.vm.provision "shell", path: "vagrant/zk.sh", :args => [i.to_s, num_zookeepers, zk_jmx_port]
    end
  }

  (1..num_brokers).each { |i|
    name = "broker" + i.to_s
    config.vm.define name do |broker|
      name_node(broker, name, ec2_instance_name_prefix)
      ip_address = "192.168.50." + (50 + i).to_s
      assign_local_ip(broker, ip_address)
      # We need to be careful about what we list as the publicly routable
      # address since this is registered in ZK and handed out to clients. If
      # host DNS isn't setup, we shouldn't use hostnames -- IP addresses must be
      # used to support clients running on the host.
      zookeeper_connect = zookeepers.map{ |zk_addr| zk_addr + ":2181"}.join(",")
      broker.vm.provision "shell", path: "vagrant/base.sh", env: {"JDK_MAJOR" => jdk_major, "JDK_FULL" => jdk_full}
      kafka_jmx_port = enable_jmx ? (9000 + i).to_s : ""
      broker.vm.provision "shell", path: "vagrant/broker.sh", :args => [i.to_s, enable_dns ? name : ip_address, zookeeper_connect, kafka_jmx_port]
    end
  }

  (1..num_workers).each { |i|
    name = "worker" + i.to_s
    config.vm.define name do |worker|
      name_node(worker, name, ec2_instance_name_prefix)
      ip_address = "192.168.50." + (100 + i).to_s
      assign_local_ip(worker, ip_address)
      worker.vm.provision "shell", path: "vagrant/base.sh", env: {"JDK_MAJOR" => jdk_major, "JDK_FULL" => jdk_full}
    end
  }

end
