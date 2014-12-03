#!/usr/bin/env bash

# this script is only a wrapper for randpermuterow_run_given_dat.sh
# you can use randpermuterow_run_given_dat.sh directly instead

# this script takes a property file as input,
# for example, configuration.txt.template,
# and run the MR-2Phase-FPGrowth program on Hadoop cluster

# this script assumes you are using hadoop-2.2.0
# we divide the nodes into two groups:
# 1) hadoop cluster node
# 2) other node
# this script can run on both type of nodes,
# if you run on other node, then you need to make sure
# this node can SSH to hadoop cluster node without password.
# this script assumes you have a hadoop copy in $HOME directory
# this script assumes the hadoop cluster is configured in this way:
# http://blog.csdn.net/licongcong_0224/article/details/12972889
# this script won't ask you for output path, because
# we use hdfs://$worknode:9000/output-test as output path 
# please use
# ./hadoop-2.2.0/bin/hdfs dfs -ls /output-test
# for output information.

# hadoop cluster is built on top of a cluster,
# that is to say, nodes that talk to each other
# by hostname in its /etc/hosts, and mutually
# trust each other so that SSH doesn't need password

set -x

absme=`readlink -f $0`
abshere=`dirname $absme`

cd $abshere

if [ $# != 1 ]; then
  echo `basename $0` /path/to/config
  exit 1
fi

configfile=$1

if [ ! -e $configfile ]; then
  echo No such file: $configfile
  exit 2
fi

if [ ! -f $configfile ]; then
  echo No such file: $configfile
  exit 3
fi

args=

while read line; do
  [[ $line =~ \#.* ]] && continue
  [[ $line =~ ^$ ]] && continue

  key=$(echo $line | cut -f1 -d' ')
  value=$(echo $line | cut -f2 -d' ')
  
  case $key in
    noupload)
      if [ $value = "yes" ]; then
        args=$args"-n "
      fi
      ;;
    minsup)
      args=$args"-m $value "
      ;;
    datname)
      args=$args"-d $value "
      ;;
    worknode)
      args=$args"-w $value "
      ;;
    user)
      args=$args"-u $value "
      ;;
    skip)
      if [ $value = "yes" ]; then
        args=$args"-s "
      fi
      ;;
    phase1minsup)
      args=$args"-x $value "
      ;;
    phase1minsupbeta)
      args=$args"-y $value "
      ;;
    solution1)
      if [ $value = "yes" ]; then
        args=$args"-j "
      fi
      ;;
    solution1param1)
      args=$args"-k $value "
      ;;
    solution1param2)
      args=$args"-v $value "
      ;;
    solution1param3)
      args=$args"-z $value "
      ;;
    inpath)
      args=$args"-a $value "
      ;;
    outpath)
      args=$args"-b $value "
      ;;
    *)
      echo haha
      ;;
  esac
done < <(cat $configfile)

echo $args
./randpermuterow_run_given_dat.sh $args

set +x
