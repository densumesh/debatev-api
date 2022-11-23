#!/bin/bash
sudo yum update
sudo amazon-linux-extras install epel -y
yum install pandoc
pandoc -v