#!/usr/bin/env bash

# this script assumes the hadoop cluster
# is installed in this way:
# http://blog.csdn.net/licongcong_0224/article/details/12972889
# hadoop folder is under home directory
# hadoop version is 2.2.0

# default value stage
deletelogs=no #l
deletedata=no #d
# the following makes lots of assumptions
# to the cluster arch, but it works
# for me so far
masternode=ibmvm1 #m
slavenodes=ibmvm1,ibmvm2,ibmvm3 #s
user=dachuan  #u
bruteforce= #b

# definition, parsing, interrogation stages
while getopts ":m:s:u:ldb" o; do
  case $o in
    m)
      masternode=$OPTARG
      ;;
    s)
      slavenodes=$OPTARG
      ;;
    u)
      user=$OPTARG
      ;;
    l)
      deletelogs="yes"
      ;;
    d)
      deletedata="yes"
      ;;
    b)
      bruteforce="yes"
      ;;
    *)
      echo Invalid arguments >&2
      ;;
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo deletelogs=$deletelogs
echo deleteadata=$deletedata
echo masternode=$masternode
echo slavenodes=$slavenodes
echo user=$user
echo bruteforce=$bruteforce

# verify arguments stage (skip)

set -x

absme=`readlink -f $0`
abshere=`dirname $absme`

cd $abshere

# main logic

IFS=', ' read -a slaves <<< $slavenodes

ssh -n $user@$masternode ./hadoop-2.2.0/sbin/stop-yarn.sh
ssh -n $user@$masternode ./hadoop-2.2.0/sbin/stop-dfs.sh

if [ $bruteforce = "yes" ]; then
  for slave in "${slaves[@]}"; do
    ssh -n $user@$slave killall -9 java
  done
fi

if [ $deletelogs = "yes" ]; then
  for slave in "${slaves[@]}"; do
    ssh -n $user@$slave rm -r -f hadoop-2.2.0/logs/*
  done
fi

if [ $deletedata = "yes" ]; then
  for slave in "${slaves[@]}"; do
    ssh -n $user@$slave rm -r -f dfs/name/* dfs/data/* temp/*
  done
  ssh -n $user@$masternode ./hadoop-2.2.0/bin/hadoop namenode -format
fi

ssh -n $user@$masternode ./hadoop-2.2.0/sbin/start-yarn.sh
ssh -n $user@$masternode ./hadoop-2.2.0/sbin/start-dfs.sh

echo Masternode jps
echo $user@$masternode && ssh -n $user@$masternode jps

echo
echo Slavenode jps
for slave in "${slaves[@]}"; do
  echo $user@$slave && ssh -n $user@$slave jps
done

set +x
