#!/bin/bash

set -e

aws s3 cp s3://yernt-bgdata/common-crawl/additional_libraries/requirements.txt .

sudo pip3 install -r requirements.txt