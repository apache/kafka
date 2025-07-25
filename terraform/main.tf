# Copyright 2024 Confluent Inc.
locals {
  common_tags = {
    "Name": "ccs-kafka-worker",
    "ducktape": "true",
    "Owner": "ce-kafka",
    "role": "ce-kafka",
    "cflt_environment": "devel",
    "cflt_partition": "onprem",
    "cflt_managed_by": "iac",
    "cflt_managed_id": "ce-kafka",
    "cflt_service": "ce-kafka"
  }
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.ec2_region
  default_tags {
    tags = local.common_tags
  }
}

data "template_file" "user_data" {
  template = file("./cloudinit.yml")
  vars = {
    deployment  = var.deployment
    public_key  = var.public_key
    base_script = data.local_file.base_script.content
  }
}

data "cloudinit_config" "user_data" {
  gzip          = false
  base64_encode = true

  part {
    content_type = "text/cloud-config"
    content      = data.template_file.user_data.rendered
  }
}

data "local_file" "base_script" {
  filename = "../vagrant/base.sh"
}

resource "aws_instance" "worker" {
  count= var.num_workers
  ami= var.worker_ami
  instance_type = var.instance_type
  key_name = "semaphore-muckrake"
  subnet_id = var.subnet_id
  vpc_security_group_ids = [var.security_group]
  associate_public_ip_address = false
  iam_instance_profile = "semaphore-access"
  user_data = data.cloudinit_config.user_data.rendered
  ipv6_address_count = var.ipv6_address_count
  tags = {
    Name = format("ccs-kafka-%d", count.index),
    "SemaphoreBuildUrl":var.build_url,
    "SemaphoreWorkflowUrl":var.build_url,
    "SemaphoreJobId": var.job_id
  }
}

output "worker-public-ips" { value = aws_instance.worker.*.public_ip }
output "worker-ipv6s" { value = aws_instance.worker.*.ipv6_addresses }
output "worker-public-dnss" { value = aws_instance.worker.*.public_dns }
output "worker-private-ips" { value = aws_instance.worker.*.private_ip }
output "worker-private-dnss" { value = aws_instance.worker.*.private_dns }
output "worker-names" { value = aws_instance.worker.*.tags.Name }
output "worker-instance-ids" { value =  aws_instance.worker.*.id }
output "cloudinit_content" { value= data.cloudinit_config.user_data.rendered}