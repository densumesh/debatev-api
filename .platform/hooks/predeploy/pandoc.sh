#!/bin/bash
sudo yum update
sudo amazon-linux-extras install epel
yum install pandoc
pandoc -v