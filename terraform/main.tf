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
}

data "template_file" "user_data" {
  template = file("./cloudinit.yml")
  vars = {
    deployment = var.deployment
    public_key = var.public_key
  }
}

data "cloudinit_config" "user_data" {
  gzip          = false
  base64_encode = true

  part {
    content_type = "text/cloud-config"
    content      = data.template_file.user_data.rendered
    filename     = "cloudinit.yml"
  }
}


resource "aws_spot_instance_request" "worker" {
    spot_price = var.ec2_spot_max_price
    wait_for_fulfillment = true
    spot_type = "one-time"
    valid_until = "{{ spot_instance_valid_time }}"
  
}

resource "aws_instance" "worker" {
    count= var.num_workers
    ami= var.ec2_ami
    instance_type = var.ec2_instance_type
    key_name = var.aws_keypair_name
    iam_instance_profile = "jenkins-master"
    subnet_id = var.ec2_subnet_id
    vpc_security_group_ids = [sg-0291bf7b2f81189ee]
    associate_public_ip_address = true
  
}


resource "null_resource" "spot_instance_tag_command" {
  triggers = {
    cluster_instance_ids = "${join(",", aws_spot_instance_request.worker.*.spot_instance_id)}"
  }
  provisioner "local-exec" {
    command = "aws ec2 create-tags --resources ${join(" ", aws_spot_instance_request.worker.*.spot_instance_id)} --tags {{ aws_tags }}"
  }
}

/*
# Terraform configuration for VirtualBox VM
provider "virtualbox" {
  # Provider-specific configurations can be defined here

}

resource "null_resource" "virtualbox_configuration" {
  # This resource doesn't actually create anything, but acts as a placeholder
  # for running local-exec provisioners.

  # Trigger the execution whenever any of the following values change
  triggers = {
    base_box      = var.base_box
    ram_megabytes = var.ram_megabytes
  }

  provisioner "local-exec" {
    command = <<-EOT
      VBoxManage modifyvm <virtualbox> --memory ${var.ram_megabytes}
    EOT
  }

  # Note: There isn't a direct equivalent to vagrant-cachier in Terraform
  # You may need to handle caching separately outside of Terraform
}

resource "virtualbox_vm" "example_vm" {
  # other VM configurations...

  # Set the cache scope if the vagrant-cachier plugin is installed
  override = var.has_vagrant_cachier_plugin ? {
    cache = "box"
  } : {}
} 


# Override the instance type if the INSTANCE_TYPE environment variable is set
locals {
  overridden_instance_type = coalesce(var.INSTANCE_TYPE, var.ec2_instance_type) //coalesce will return first non-null value from left to right 
}

# Choose size based on the overridden instance type
locals {
  ebs_volume_size = regex("^m3.*", local.overridden_instance_type) ? 20 : 40  #regrex will check the regular expression as "m3" starts with
}
*/