#!/bin/bash
sudo yum update
yum install epel-release
yum install pandoc
pandoc -v