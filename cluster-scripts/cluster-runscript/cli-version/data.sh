#!/usr/bin/env bash

# default value stage
folder= #f
noupload= #n
worknode=ibmvm1 #w
user=dachuan  #u

# definition, parsing, interrogation stages
while getopts ":f:w:u:n" o; do
  case $o in
    f) 
      folder=$OPTARG
      ;;
    w)
      worknode=$OPTARG
      ;;
    u)
      user=$OPTARG
      ;;
    n)
      noupload=noupload
      ;;
    *)
      echo invalid argument >&2
      exit 1
      ;;
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo folder=$folder
echo noupload=$noupload
echo worknode=$worknode
echo user=$user

#verify arguments stage (skip)

# standard header
absme=`readlink -f $0`
abshere=`dirname $absme`

# argument path absolutify
folder=`readlink -f $folder`
foldername=`basename $folder`

# enter my work directory
cd $abshere

# main logic
set -x

ssh -n $user@$worknode /home/$user/hadoop-2.2.0/bin/hdfs dfs -rm -r -f /output-test-1stphase /output-test

if [ "$noupload" = "noupload" ]; then
  echo Data upload is skipped, user assumes the input is ready in HDFS
else
  ssh -n $user@$worknode "rm -rf /tmp/${foldername}dif && mkdir /tmp/${foldername}dif"
  echo start uploading data to hdfs && date
  scp -r $folder/* $user@$worknode:/tmp/${foldername}dif/
  echo data uploaded to hdfs && date
  ssh -n $user@$worknode ./hadoop-2.2.0/bin/hdfs dfs -rm -r -f /input-test

  ssh -n $user@$worknode ./hadoop-2.2.0/bin/hdfs dfs -mkdir /input-test
  ssh -n $user@$worknode ./hadoop-2.2.0/bin/hdfs dfs -copyFromLocal /tmp/${foldername}dif/* /input-test/

fi

set +x
