#!/bin/sh

# Update script
cd /home/ubuntu/AdNet

# Pulls down the latest changes
git pull origin master

# Installs project dependencies
npm install
sudo pip install -r requirements.txt
