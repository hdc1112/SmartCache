#!/usr/bin/env bash

echo "This script is out of date for a long time"
echo "Update this script by following randpermuterow_run_given_dat.sh"
exit

# default value stage
permutefile=  #f
tolerate=
noupload=
minsupport=
enableopt1=
enableopt2=
datname=
worknode=ibmvm1
user=dachuan

# definition, parsing, interrogation stages
while getopts ":d:f:t:m:w:u:pqn" o; do
  case $o in 
    d)
      datname=$OPTARG
      ;;
    f)
      permutefile=$OPTARG
      ;;
    t)
      tolerate=$OPTARG
      ;;
    m)
      minsupport=$OPTARG
      ;;
    w)
      worknode=$OPTARG
      ;;
    u)
      user=$OPTARG
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
    *)
      echo invalid argument >&2
      exit 1
      ;;
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo datname=$datname
echo permutefile=$permutefile
echo tolerate=$tolerate
echo noupload=$noupload
echo minsupport=$minsupport
echo enableopt1=$enableopt1
echo enableopt2=$enableopt2
echo worknode=$worknode
echo user=$user

# verify arguments stage (skip)

# standard header
absme=`readlink -f $0`
abshere=`dirname $absme`

# arg path absolutify

# enter wkdir
cd $abshere

# main logic
set -x
platform=`uname -o`
storedir=/tmp
filename=$datname.dat
transf=$storedir/$filename.transf

if [ ! -f $transf ]; then
  echo did not find $transf, transform now
  ./transform_given_dat.sh -d $datname
elif [ `cat $storedir/$filename | wc -l` != `cat $transf | wc -l` ]; then
  echo found $transf, but it is corrupted
  ./transform_given_dat.sh -d $datname
else
  echo found $transf, it looks good
fi

filenameastold=$transf-permuteastold

permutefile=`readlink -f $permutefile`

cd $abshere/../../../src
javac PermuteRowsAsTold.java
if [ $platform = "Cygwin" ]; then
  java PermuteRowsAsTold `cygpath -wp $transf` `cygpath -wp $permutefile`
else
  java PermuteRowsAsTold $transf $permutefile
fi
cd $abshere

totallinenum=`cat $storedir/$filename | wc -l`
linenum=$totallinenum
#linenum=$((linenum/2))
#linenum=$((linenum/2))
#linenum=$((linenum/2))

realfile=$filenameastold

halflinenum=$((linenum/2))
columns=`head -1 $transf | awk '{print NF}'`
echo columns=$columns

datapath=/tmp/tempdatafolder/
rm -rf $datapath
if [ ! -d $datapath ]; then
  mkdir $datapath
fi
cat $realfile | head -n $halflinenum > $datapath/1.txt
cat $realfile | head -n $linenum | tail -n $halflinenum > $datapath/2.txt

diff -q $datapath/1.txt $datapath/2.txt

./run.sh -d $datapath -c $columns -m $minsupport -t $tolerate $enableopt1 $enableopt2 $noupload -w $worknode -u $user
date

set +x
