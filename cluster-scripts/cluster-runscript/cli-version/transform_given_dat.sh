#!/usr/bin/env bash

# this script accepts the dataset name,
# and assume this dataset can be fetched
# from http://fimi.ua.ac.be/data/

# default value stage
datname=  #d
transform=no  #t

# definition, parsing, interrogation stages
while getopts ":d:t" o; do
  case $o in
    d)
      datname=$OPTARG
      ;;
    t)
      transform=yes
      ;;
    *)
      echo invalid argument >&2
      ;;
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo datname=$datname

platform=`uname -o`
storedir=/tmp
filename=$datname.dat

absme=`readlink -f $0`
abshere=`dirname $absme`

if [ ! -f $storedir/$filename ]; then
  cd $storedir
  wget http://fimi.ua.ac.be/data/$datname.dat
fi

cd $abshere/../../../src

if [ $transform = "yes" ]; then
  javac ToBoolMatrix.java
  if [ $platform = "Cygwin" ]; then
    java ToBoolMatrix `cygpath -wp $storedir/$filename` > `cygpath -wp $storedir/$filename.transf`
  else 
    java ToBoolMatrix $storedir/$filename > $storedir/$filename.transf
  fi
fi
