#!/usr/bin/env bash

# default value stage
dataabspath=  #d
columns=  #c
minsupport= #m
tolerate= #t
enableopt1= #p
enableopt2= #q
noupload= #n
worknode=ibmvm1 #w
user=dachuan  #u
phase1minsup= #x
phase1minsupbeta= #y
solution1=  #j
solution1param1=  #k
solution1param2=  #r
solution1param3=  #s
inpath= #a
outpath=  #b

# definition, parsing, interrogation stages
while getopts ":a:b:d:c:m:t:w:u:x:y:k:r:s:pqnj" o; do
  case $o in
    a)
      inpath="-i $OPTARG"
      ;;
    b)
      outpath="-o $OPTARG"
      ;;
    d)
      dataabspath="-f $(readlink -f $OPTARG)"
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
      ;;
    u)
      user=$OPTARG
      ;;
    x)
      phase1minsup="-x $OPTARG"
      ;;
    y)
      phase1minsupbeta="-y $OPTARG"
      ;;
    k)
      solution1param1="-k $OPTARG"
      ;;
    r)
      solution1param2="-r $OPTARG"
      ;;
    s)
      solution1param3="-s $OPTARG"
      ;;
    p)
      enableopt1="-p"
      ;;
    q)
      enableopt2="-q"
      ;;
    n)
      noupload="-n"
      ;;
    j)
      solution1="-j"
      ;;
    *)
      echo invalid argument >&2
      exit 1
      ;;
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo dataabspath=$dataabspath
echo columns=$columns
echo minsupport=$minsupport
echo tolerate=$tolerate
echo enableopt1=$enableopt1
echo enableopt2=$enableopt2
echo noupload=$noupload
echo worknode=$worknode
echo user=$user
echo phase1minsup=$phase1minsup
echo phase1minsupbeta=$phase1minsupbeta
echo solution1=$solution1
echo solution1param1=$solution1param1
echo solution1param2=$solution1param2
echo solution1param3=$solution1param3
echo inpath=$inpath
echo outpath=$outpath

# verify arguments stage (skip)

# standard header
absme=`readlink -f $0`
abshere=`dirname $absme`

# argument path absolutify
#dataabspath=`readlink -f $dataabspath`

# enter my work directory
cd $abshere

# main logic
set -x

./jar.sh -w $worknode -u $user
./data.sh $dataabspath $noupload -w $worknode -u $user
#./remote-run.sh -c $columns -m $minsupport -t $tolerate $enableopt1 $enableopt2 -w $worknode -u $user
./remote-run.sh -m $minsupport $enableopt1 $enableopt2 -w $worknode -u $user $phase1minsup $phase1minsupbeta $solution1 $solution1param1 $solution1param2 $solution1param3 $inpath $outpath

set +x
