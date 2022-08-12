#!/bin/bash
set -o errexit
# Date : 14:43 2020-03-17
# Author : Created by lishichao
# log : null
# Description   : This scripts function is a check
# Version : 1.0
# demo sh PhoenixLoadMetaData.sh example.json 1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $DIR
$DIR/../python/python $DIR/../script/PhoenixLoadMetaData.py $1 $2
