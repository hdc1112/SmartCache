#!/usr/bin/env bash

# default value stage
columns=  #c
minsupport= #m
tolerate= #t
enableopt1= #p
enableopt2= #q
worknode=ibmvm1 #w
user=dachuan  #u
inpath=hdfs://$worknode:9000/input-test  #i
outpath=hdfs://$worknode:9000/output-test   #o
phase1minsup= #x
phase1minsupbeta= #y
solution1=  #j
solution1param1=  #k
solution1param2=  #r
solution1param3=  #s

# definition, parsing, interrogation stages
while getopts ":i:o:c:m:t:w:u:x:y:k:r:s:pqj" o; do
  case $o in
    i)
      inpath=$OPTARG
      ;;
    o)
      outpath=$OPTARG
      ;;
    c)
      columns=$OPTARG
      ;;
    m)
      minsupport=$OPTARG
      ;;
    t)
      tolerate=$OPTARG
      ;;
    w)
      worknode=$OPTARG
      inpath=hdfs://$worknode:9000/input-test 
      outpath=hdfs://$worknode:9000/output-test 
      ;;
    u)
      user=$OPTARG
      ;;
    x)
      phase1minsup="--phase1minsup $OPTARG"
      ;;
    y)
      phase1minsupbeta="--phase1minsupbeta $OPTARG"
      ;;
    k)
      solution1param1="--solution1param1 $OPTARG"
      ;;
    p)
      enableopt1="--enableOPT1"
      ;;
    q)
      enableopt2="--enableOPT2"
      ;;
    j)
      solution1="--solution1"
      ;;
    r)
      solution1param2="--solution1param2 $OPTARG"
      ;;
    s)
      solution1param3="--solution1param3 $OPTARG"
      ;;
    *)
      echo invalid argument >&2
      exit 1
      ;;
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo inputpath=$inpath
echo outputpath=$outpath
echo columns=$columns
echo minsupport=$minsupport
echo tolerate=$tolerate
echo enableopt1=$enableopt1
echo enableopt2=$enableopt2
echo worknode=$worknode
echo user=$user
echo phase1minsup=$phase1minsup
echo phase1minsupbeta=$phase1minsupbeta
echo solution1=$solution1
echo solution1param1=$solution1param1
echo solution1param2=$solution1param2
echo solution1param3=$solution1param3

# verify arguments stage, exit if necessary (skip)

# standard header
absme=`readlink -f $0`
abshere=`dirname $absme`

# argument path absolutify

# enter my work directory
cd $abshere

# main logic
set -x

#echo "/home/$user/hadoop-2.2.0/bin/hadoop jar /tmp/mr-2phase-fpgrowth.jar --inpath $inpath --outpath $outpath --columns $columns --minsupport $minsupport --tolerate $tolerate $enableopt1 $enableopt2" > /tmp/remote-exe.sh
echo "/home/$user/hadoop-2.2.0/bin/hadoop jar /tmp/mr-2phase-fpgrowth.jar --inpath $inpath --outpath $outpath --minsupport $minsupport $phase1minsup $phase1minsupbeta $solution1 $solution1param1 $solution1param2 $solution1param3" > /tmp/remote-exe.sh
scp /tmp/remote-exe.sh $user@$worknode:/tmp/
ssh -n $user@$worknode chmod +x /tmp/remote-exe.sh
ssh -n $user@$worknode /tmp/remote-exe.sh

set +x
