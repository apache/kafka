variable "ec2_region" {
  type    = string
  description = "EC2 region"
  default = "us-west-2"
}

variable "num_workers" {
  type        = string
  description = "number of workers"
}

variable "worker_ami" {
  type        = string
  description = "AMI of aws"
}

variable "instance_type" {
  type        = string
  description = "Instance type of aws"
  default = "c4.xlarge"
}

variable "deployment" {
  type        = string
  default     = "ubuntu"
  description = "linux distro you are deploying with, valid values are ubuntu and rpm"
}

variable "public_key" {
  type        = string
  description = "kafka pem file public key"
}

variable "build_url" {
  type = string
}

variable "job_id" {
  type = string
  description = "semaphore job id"
}

variable "subnet_id" {
  type = string
  description = "subnet id"
}

variable "ipv6_address_count" {
  type = number
  description = "number of ipv6 addresses"
}

variable "security_group" {
  type = string
  description = "security group id"
  default = "sg-03364f9fef903b17d"
}