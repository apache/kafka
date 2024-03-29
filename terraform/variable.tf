variable "ec2_region" {
  type    = string
  description = "EC2 region"
  default = "us-west-1"
}

variable "ec2_spot_max_price" {
  type = string
}

variable "num_workers" {
  type        = string
  description = "number of workers"
}

variable "ec2_ami" {
  type        = string
  description = "AMI of aws"
}

variable "ec2_instance_type" {
  type        = string
  description = "Instance type of aws"
}

variable "aws_keypair_name" {
  type    = string
  description = "Key name of aws"
}

variable "ec2_subnet_id" {
  type = string
  description = "Subnet ID of aws"
}

variable "deployment" {
  type        = string
  default     = "ubuntu"
  description = "linux distro you are deploying with, valid values are ubuntu and rpm"
}

variable "public_key" {
  type        = string
  description = "muckrake pem file public key"
}