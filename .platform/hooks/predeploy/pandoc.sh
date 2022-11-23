#!/bin/bash
sudo yum -y update 
sudo amazon-linux-extras -y install epel 
sudo yum repolist
yum -y install pandoc 
pandoc -v