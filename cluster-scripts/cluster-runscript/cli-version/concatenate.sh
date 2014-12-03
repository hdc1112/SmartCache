#!/usr/bin/env bash

platform=`uname -o`

retval=""

while read line
do
  if [ $platform = "Cygwin" ]; then
    retval=`cygpath -wp $line`\;$retval
  else
    retval=$line:$retval
  fi
done < "${1:-/proc/${$}/fd/0}"

echo $retval
