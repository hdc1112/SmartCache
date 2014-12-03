#!/usr/bin/env bash

# this script is based on the relative path
# to the src/ folder, so don't move this script
# unless you know what you are doing

# this script assumes the main java class
# is in the src/ root folder, so don't move the
# main java file to other folder

# default value stage
worknode=ibmvm1 #w
user=dachuan  #u

# definition, parsing, interrogation stages
while getopts ":w:u:" o; do
  case $o in
    w)
      worknode=$OPTARG
      ;;
    u)
      user=$OPTARG
      ;;
    *)
      echo invalid argument >&2
      exit 1
      ;;
  esac
done

# arguments show stage
echo worknode=$worknode
echo user=$user

# verify arguments stage (skip)

set -x

absme=`readlink -f $0`
abshere=`dirname $absme`

cd $abshere/../../../src

if [ ! -f hadoopclasspath.txt ]; then
  find $HOME/hadoop-2.2.0/share/hadoop -type f -name "*.jar" | $abshere/concatenate.sh > hadoopclasspath.txt
fi
classes=`cat hadoopclasspath.txt`
javac -classpath $classes *.java
#jar cvfe ./mr-2phase-fpgrowth.jar MR2PhaseFPGrowth *.class
jar cvfe ./mr-2phase-fpgrowth.jar MR2PhaseFPGrowth -C . .
jar -tf ./mr-2phase-fpgrowth.jar
scp ./mr-2phase-fpgrowth.jar $user@$worknode:/tmp/
#rm -f *.class
find -type f -name "*.class" | xargs rm -f

set +x
