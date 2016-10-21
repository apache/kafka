# Apache Kafka #

Using Vagrant to get up and running.

1) Install Virtual Box [https://www.virtualbox.org/](https://www.virtualbox.org/)
2) Install Vagrant >= 1.6.4 [http://www.vagrantup.com/](http://www.vagrantup.com/)
3) Install Vagrant Plugins:

    $ vagrant plugin install vagrant-hostmanager
    # Optional
    $ vagrant plugin install vagrant-cachier # Caches & shares package downloads across VMs

In the main Kafka folder, do a normal Kafka build:

    $ gradle
    $ ./gradlew jar

You can override default settings in `Vagrantfile.local`, which is a Ruby file
that is ignored by git and imported into the Vagrantfile.
One setting you likely want to enable
in `Vagrantfile.local` is `enable_dns = true` to put hostnames in the host's
/etc/hosts file. You probably want this to avoid having to use IP addresses when
addressing the cluster from outside the VMs, e.g. if you run a client on the
host. It's disabled by default since it requires `sudo` access, mucks with your
system state, and breaks with naming conflicts if you try to run multiple
clusters concurrently.

Now bring up the cluster:

    $ vagrant/vagrant-up.sh     
    $ # If on aws, run: vagrant/vagrant-up.sh --aws

(This essentially runs vagrant up --no-provision && vagrant hostmanager && vagrant provision)

We separate out the steps (bringing up the base VMs, mapping hostnames, and configuring the VMs)
due to current limitations in ZooKeeper (ZOOKEEPER-1506) that require us to
collect IPs for all nodes before starting ZooKeeper nodes. Breaking into multiple steps
also allows us to bring machines up in parallel on AWS.

Once this completes:

* Zookeeper will be running on 192.168.50.11 (and `zk1` if you used enable_dns)
* Broker 1 on 192.168.50.51 (and `broker1` if you used enable_dns)
* Broker 2 on 192.168.50.52 (and `broker2` if you used enable_dns)
* Broker 3 on 192.168.50.53 (and `broker3` if you used enable_dns)

To log into one of the machines:

    vagrant ssh <machineName>

You can access the brokers and zookeeper by their IP or hostname, e.g.

    # Specify ZooKeeper node 1 by it's IP: 192.168.50.11
    bin/kafka-topics.sh --create --zookeeper 192.168.50.11:2181 --replication-factor 3 --partitions 1 --topic sandbox

    # Specify brokers by their hostnames: broker1, broker2, broker3
    bin/kafka-console-producer.sh --broker-list broker1:9092,broker2:9092,broker3:9092 --topic sandbox

    # Specify ZooKeeper node by its hostname: zk1
    bin/kafka-console-consumer.sh --zookeeper zk1:2181 --topic sandbox --from-beginning

If you need to update the running cluster, you can re-run the provisioner (the
step that installs software and configures services):

    vagrant provision

Note that this doesn't currently ensure a fresh start -- old cluster state will
still remain intact after everything restarts. This can be useful for updating
the cluster to your most recent development version.

Finally, you can clean up the cluster by destroying all the VMs:

    vagrant destroy -f

## Configuration ##

You can override some default settings by specifying the values in
`Vagrantfile.local`. It is interpreted as a Ruby file, although you'll probably
only ever need to change a few simple configuration variables. Some values you
might want to override:

* `enable_hostmanager` - true by default; override to false if on AWS to allow parallel cluster bringup.
* `enable_dns` - Register each VM with a hostname in /etc/hosts on the
  hosts. Hostnames are always set in the /etc/hosts in the VMs, so this is only
  necessary if you want to address them conveniently from the host for tasks
  that aren't provided by Vagrant.
* `enable_jmx` - Whether to enable JMX ports on 800x and 900x for Zookeeper and the Brokers respectively where `x` is the nodes of each respectively. For example, the zk1 machine would have JMX exposed on 8001, ZK2 would be on 8002, etc. 
* `num_workers` - Generic workers that get the code (from this project), but don't start any services (no brokers, no zookeepers, etc). Useful for starting clients. Each worker will have an IP address of `192.168.50.10x` where `x` starts at `1` and increments for each worker. 
* `num_zookeepers` - Size of zookeeper cluster
* `num_brokers` - Number of broker instances to run
* `ram_megabytes` - The size of each virtual machine's RAM; default to `1200MB`



## Using Other Providers ##

### EC2 ###

Install the `vagrant-aws` plugin to provide EC2 support:

    $ vagrant plugin install vagrant-aws

Next, configure parameters in `Vagrantfile.local`. A few are *required*:
`enable_hostmanager`, `enable_dns`, `ec2_access_key`, `ec2_secret_key`, `ec2_keypair_name`, `ec2_keypair_file`, and
`ec2_security_groups`. A couple of important notes:

1. You definitely want to use `enable_dns` if you plan to run clients outside of
   the cluster (e.g. from your local host). If you don't, you'll need to go
   lookup `vagrant ssh-config`.

2. You'll have to setup a reasonable security group yourself. You'll need to
   open ports for Zookeeper (2888 & 3888 between ZK nodes, 2181 for clients) and
   Kafka (9092). Beware that opening these ports to all sources (e.g. so you can
   run producers/consumers locally) will allow anyone to access your Kafka
   cluster. All other settings have reasonable defaults for setting up an
   Ubuntu-based cluster, but you may want to customize instance type, region,
   AMI, etc.

3. `ec2_access_key` and `ec2_secret_key` will use the environment variables
   `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` respectively if they are set and not
   overridden in `Vagrantfile.local`.

4. If you're launching into a VPC, you must specify `ec2_subnet_id` (the subnet
   in which to launch the nodes) and `ec2_security_groups` must be a list of
   security group IDs instead of names, e.g. `sg-34fd3551` instead of
   `kafka-test-cluster`.

Now start things up, but specify the aws provider:

    $ vagrant/vagrant-up.sh

Your instances should get tagged with a name including your hostname to make
them identifiable and make it easier to track instances in the AWS management
console.
