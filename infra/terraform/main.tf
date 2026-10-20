terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 5.92"
        }
    }

    required_version = ">= 1.2"
}

provider "aws" {
    region = "us-west-2"
}

data "aws_ami" "ubuntu" {
    most_recent = true

    filter {
        name = "name"
        values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
    }

    owners = ["099720109477"]
}
