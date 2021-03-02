#!/bin/sh

set -eux

apt-get update
apt-get install -y libtinfo5

npm install
