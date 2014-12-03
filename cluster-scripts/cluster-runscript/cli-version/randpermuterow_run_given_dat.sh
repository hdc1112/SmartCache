#!/usr/bin/env bash

# default value stage
tolerate= #t
noupload= #n
minsupport= #m
enableopt1= #p
enableopt2= #q
datname=  #d
permutefile=    #o
worknode=ibmvm1 #w
user=dachuan  #u
skip= #s, skip permuting rows
phase1minsup= #x 
phase1minsupbeta= #y
solution1=  #j
solution1param1=  #k
solution1param2=  #v
solution1param3=  #z
inpath= #a
outpath=  #b

# definition, parsing, interrogation stages
while getopts ":d:t:m:o:w:u:x:y:k:v:z:a:b:snpqj" o; do
  case $o in
    a)
      inpath="-a $OPTARG"
      ;;
    b)
      outpath="-b $OPTARG"
      ;;
    d)
      datname=$OPTARG
      ;;
    t)
      tolerate=$OPTARG
      ;;
    m)
      minsupport=$OPTARG
      ;;
    o)
      permutefile=$OPTARG
      ;;
    w)
      worknode=$OPTARG
      ;;
    u)
      user=$OPTARG
      ;;
    s)
      skip=yes
      ;;
    x)
      phase1minsup="-x $OPTARG"
      ;;
    y)
      phase1minsupbeta="-y $OPTARG"
      ;;
    n)
      noupload="-n"
      ;;
    p)
      enableopt1="-p"
      ;;
    q)
      enableopt2="-q"
      ;;
    j)
      solution1="-j"
      ;;
    k)
      solution1param1="-k $OPTARG"
      ;;
    v)
      solution1param2="-r $OPTARG"
      ;;
    z)
      solution1param3="-s $OPTARG"
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
echo tolerate=$tolerate
echo noupload=$noupload
echo minsupport=$minsupport
echo enableopt1=$enableopt1
echo enableopt2=$enableopt2
echo permutefile=$permutefile
echo worknode=$worknode
echo user=$user
echo skip=$skip
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

# arg path absolutify

# entery wkdir
cd $abshere

# main logic
set -x

platform=`uname -o`

if [ -z $noupload ]; then

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

  totallinenum=`cat $storedir/$filename | wc -l`
  echo totallinenum=$totallinenum
  linenum=$totallinenum
  #linenum=$((linenum/2))
  #linenum=$((linenum/2))
  #linenum=$((linenum/2))

  #realfile=$transf.realfile
  realfile=$storedir/$filename.realfile
  #cat $transf | head -n $linenum > $realfile
  cat $storedir/$filename | head -n $linenum > $realfile

  if [ -z $skip ]; then
    cd $abshere/../../../src
    if [ ! -f hadoopclasspath.txt ]; then
      find $HOME/hadoop-2.2.0/share/hadoop -type f -name "*.jar" | $abshere/concatenate.sh > hadoopclasspath.txt
    fi
    classes=`cat hadoopclasspath.txt`
    javac -classpath $classes PermuteRows.java
    if [ -z $permutefile ]; then
      if [ $platform = "Cygwin" ]; then
        java -classpath $classes PermuteRows --datafile `cygpath -wp $realfile`
      else
        java -classpath $classes PermuteRows --datafile $realfile
      fi
    else
      rm -f $permutefile
      if [ $platform = "Cygwin" ]; then
        java -classpath $classes PermuteRows --datafile `cygpath -wp $realfile` --permutefile `cygpath -wp $permutefile`
      else
        java -classpath $classes PermuteRows --datafile $realfile --permutefile $permutefile
      fi
    fi
    cd $abshere

    realfile=${realfile}-randpermute
  fi

  halflinenum=$((linenum/2))
  #columns=`head -1 $transf | awk '{print NF}'`
  #echo columns=$columns

  datapath=/tmp/tempdatafolder/
  rm -rf $datapath
  if [ ! -d $datapath ]; then
    mkdir $datapath
  fi
  cat $realfile | head -n $halflinenum > $datapath/1.txt
  cat $realfile | head -n $linenum | tail -n $halflinenum > $datapath/2.txt

  diff -q $datapath/1.txt $datapath/2.txt

fi

datafolder=
if [ ! -z $datapath ]; then
  datafolder="-d $datapath"
fi

#./run.sh -d $datapath -c $columns -m $minsupport -t $tolerate $enableopt1 $enableopt2 $noupload -w $worknode -u $user
./run.sh $datafolder -m $minsupport $enableopt1 $enableopt2 $noupload -w $worknode -u $user $phase1minsup $phase1minsupbeta $solution1 $solution1param1 $solution1param2 $solution1param3 $inpath $outpath
date

set +x
