#!/usr/bin/env bash

# default value stage
running=
executions=

# definition, parsing, interrogation stages
while getopts ":re:" o; do
  case $o in
    r)
      running=yes
      ;;
    e)
      executions=$OPTARG
      ;;
    *)
      echo invalid arguments >&2
      exit 1
  esac
done

# arguments show stage
echo `basename $0` arguments list
echo running=$running
echo executions=$executions

# lesson for future
# inside function, use "local" keyword to declare variable

# standard header
set -x
absme=`readlink -f $0`
abshere=`dirname $absme`
me=`basename $absme`

# output, and output format of this script is in 
# 6.19.2014.design.exp.chess.dat.header.file.xlsx
# and tolerate itemsets are added

# interactive input
# TODO read the backup log folder's name from user
#read -p "Server backup log folder path: " user_servbakupf
# read the stub program's log path from user
read -p "Stub program log: " stubprogram_log
if [ -z $stubprogram_log ]; then
  echo Invalid stub program log path
  exit 2
fi
if [ ! -f $stubprogram_log ]; then
  echo No such file $stubprogram_log
  exit 2
fi
stubprogram_log=`readlink -f $stubprogram_log`
# read the statistics output file path
read -p "Statistics output path: " user_statpath
if [ -z $user_statpath ]; then
  echo Invalid Statistics output path
  exit 2
fi
rm -f $user_statpath
touch $user_statpath
user_statpath=`readlink -f $user_statpath`

# preprocess the stub program log
# the following code works under the assumption that all
# run's stub log is incorrupted, and in a sequential
# order. and server logs are also like this.
# index starts from 1
# this method will prevent any future scanning to
# stub program log
echo Preprocessing the stub program log
cat $stubprogram_log | grep "${prefix}Total\ execution time:\ " > $in_localrepo/stub_t.log
cat $stubprogram_log | grep "${prefix}Phase\ 1\ execution time:\ " > $in_localrepo/stub_t1.log
cat $stubprogram_log | grep "${prefix}Phase\ 2\ execution time:\ " > $in_localrepo/stub_t2.log
loop=1
tarray=
set +x
while read line; do
  [[ $line =~ ${prefix}Total\ execution\ time:\ ([0-9]*) ]] && t=${BASH_REMATCH[1]}
  tarray[$loop]=${t:-NULL}
  loop=$((loop+1))
done < <(cat $in_localrepo/stub_t.log)
loop=1
t1array=
while read line; do
  [[ $line =~ ${prefix}Phase\ 1\ execution\ time:\ ([0-9]*) ]] && t1=${BASH_REMATCH[1]}
  t1array[$loop]=${t1:-NULL}
  loop=$((loop+1))
done < <(cat $in_localrepo/stub_t1.log)
loop=1
t2array=
while read line; do
  [[ $line =~ ${prefix}Phase\ 2\ execution\ time:\ ([0-9]*) ]] && t2=${BASH_REMATCH[1]}
  t2array[$loop]=${t2:-NULL}
  loop=$((loop+1))
done < <(cat $in_localrepo/stub_t2.log)
set -x

# input of this script
# hardware configuration
# this script is only designed for mr2phaseapriori
# nodes list
in_nodes="ibmvm1 ibmvm2 ibmvm3"
in_user="dachuan"
in_sshnodes="$in_user@ibmvm1 $in_user@ibmvm2 $in_user@ibmvm3"
in_localrepo=/tmp/${me}.$$.localrepo
if [ -d $in_localrepo ]; then
  rm -r -f $in_localrepo
fi
mkdir $in_localrepo
in_remotehadoop=/home/$in_user/hadoop-2.2.0
in_remotehadooplogs=$in_remotehadoop/logs
in_remotehadoopbakuplogs=/home/$in_user/hadoop-logs
for n in $in_sshnodes; do
  ssh -n $n "if [ ! -d $in_remotehadoopbakuplogs ]; then mkdir $in_remotehadoopbakuplogs; fi"
done
# this script assume log is in stderr in the following folder
in_userlogs=userlogs
# log version
logv="logv 6"
prefix="\[MR2PhaseApriori\]\[$logv\]\ "
# this script assume that the remote logs are organized in:
# logs-hadoop1 --> userlogs --> application_*_0001 --> container_*_0001 --> stderr,stdout
# this script assume that every log folder has exactly the same structure
# and no corrupted message
# this script has many implicit assumptions, such as #mappers are 2,
# #reducers are 1, no speculative, etc.
# Just remember, whenever there is some change, no matter what change,
# the first thought should be whether this script would still work.

# debug param
deb_clean=yes

# entering this script folder
cd $abshere

# under this line, there's no hard-coded thing

# functions
function get_m1_1_start {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_1_start=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Map\ [tT]ask\ start\ time:\ ([0-9]*) ]]\
      && m1_1_start=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_1_start ]; then
    break
  fi
done
echo $m1_1_start
}

function get_m1_1_end {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_1_end=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Map\ [tT]ask\ end\ time:\ ([0-9]*) ]]\
      && m1_1_end=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_1_end ]; then
    break
  fi
done
echo $m1_1_end
}

function get_m1_1_loop {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_1_loop=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Total\ loops:\ ([0-9]*) ]]\
      && m1_1_loop=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_1_loop ]; then
    break
  fi
done
echo $m1_1_loop
}

function get_m1_1 {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_1=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Map\ [tT]ask\ execution\ time:\ ([0-9]*) ]]\
      && m1_1=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_1 ]; then
    break
  fi
done
echo $m1_1
}

function get_m1_1_tolerate {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_1_tolerate=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Tolerate\ itemsets:\ ([0-9]*) ]]\
      && m1_1_tolerate=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_1_tolerate ]; then
    break
  fi
done
echo $m1_1_tolerate
}

function get_m1_2_start {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_2_start=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 1\ Map\ [tT]ask\ start\ time:\ ([0-9]*) ]]\
      && m1_2_start=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_2_start ]; then
    break
  fi
done
echo $m1_2_start
}

function get_m1_2_end {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_2_end=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 1\ Map\ [tT]ask\ end\ time:\ ([0-9]*) ]]\
      && m1_2_end=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_2_end ]; then
    break
  fi
done
echo $m1_2_end
}

function get_m1_2_loop {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_2_loop=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 1\ Total\ loops:\ ([0-9]*) ]]\
      && m1_2_loop=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_2_loop ]; then
    break
  fi
done
echo $m1_2_loop
}

function get_m1_2 {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_2=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 1\ Map\ [tT]ask\ execution\ time:\ ([0-9]*) ]]\
      && m1_2=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_2 ]; then
    break
  fi
done
echo $m1_2
}

function get_m1_2_tolerate {
l_totalcont=$1 && shift
l_interestfiles=($@)
m1_2_tolerate=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 1\ Tolerate\ itemsets:\ ([0-9]*) ]]\
      && m1_2_tolerate=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m1_2_tolerate ]; then
    break
  fi
done
echo $m1_2_tolerate
}

function get_r1_1_start {
l_totalcont=$1 && shift
l_interestfiles=($@)
r1_1_start=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Reduce\ [tT]ask\ start\ time:\ ([0-9]*) ]]\
      && r1_1_start=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $r1_1_start ]; then
    break
  fi
done
echo $r1_1_start
}

function get_r1_1_end {
l_totalcont=$1 && shift
l_interestfiles=($@)
r1_1_end=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Reduce\ [tT]ask\ end\ time:\ ([0-9]*) ]]\
      && r1_1_end=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $r1_1_end ]; then
    break
  fi
done
echo $r1_1_end
}

function get_r1_1 {
l_totalcont=$1 && shift
l_interestfiles=($@)
r1_1=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(1/2\)\ 0\ Reduce\ execution\ time:\ ([0-9]*) ]]\
      && r1_1=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $r1_1 ]; then
    break
  fi
done
echo $r1_1
}

function get_m2_1_start {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_1_start=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Map\ [tT]ask\ start\ time:\ ([0-9]*) ]]\
      && m2_1_start=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_1_start ]; then
    break
  fi
done
echo $m2_1_start
}

function get_m2_1_end {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_1_end=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Map\ [tT]ask\ end\ time:\ ([0-9]*) ]]\
      && m2_1_end=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_1_end ]; then
    break
  fi
done
echo $m2_1_end
}

function get_m2_1 {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_1=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Map\ [tT]ask\ execution\ time:\ ([0-9]*) ]]\
      && m2_1=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_1 ]; then
    break
  fi
done
echo $m2_1
}

function get_m2_1_cache_total {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_1_cache_total=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Cache\ hit:\ [0-9]*\ /\ ([0-9]*)\ =\ [01]\.[0-9]* ]]\
      && m2_1_cache_total=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_1_cache_total ]; then
    break
  fi
done
echo $m2_1_cache_total
}

function get_m2_1_cache_hit {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_1_cache_hit=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Cache\ hit:\ [0-9]*\ /\ [0-9]*\ =\ ([01]\.[0-9]*) ]]\
      && m2_1_cache_hit=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_1_cache_hit ]; then
    break
  fi
done
echo $m2_1_cache_hit
}

function get_m2_2_start {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_2_start=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 1\ Map\ [tT]ask\ start\ time:\ ([0-9]*) ]]\
      && m2_2_start=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_2_start ]; then
    break
  fi
done
echo $m2_2_start
}

function get_m2_2_end {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_2_end=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 1\ Map\ [tT]ask\ end\ time:\ ([0-9]*) ]]\
      && m2_2_end=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_2_end ]; then
    break
  fi
done
echo $m2_2_end
}

function get_m2_2 {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_2=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 1\ Map\ [tT]ask\ execution\ time:\ ([0-9]*) ]]\
      && m2_2=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_2 ]; then
    break
  fi
done
echo $m2_2
}

function get_m2_2_cache_total {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_2_cache_total=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 1\ Cache\ hit:\ [0-9]*\ /\ ([0-9]*)\ =\ [01]\.[0-9]* ]]\
      && m2_2_cache_total=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_2_cache_total ]; then
    break
  fi
done
echo $m2_2_cache_total
}

function get_m2_2_cache_hit {
l_totalcont=$1 && shift
l_interestfiles=($@)
m2_2_cache_hit=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 1\ Cache\ hit:\ [0-9]*\ /\ [0-9]*\ =\ ([01]\.[0-9]*) ]]\
      && m2_2_cache_hit=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $m2_2_cache_hit ]; then
    break
  fi
done
echo $m2_2_cache_hit
}

function get_r2_1_start {
l_totalcont=$1 && shift
l_interestfiles=($@)
r2_1_start=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Reduce\ [tT]ask\ start\ time:\ ([0-9]*) ]]\
      && r2_1_start=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $r2_1_start ]; then
    break
  fi
done
echo $r2_1_start
}

function get_r2_1_end {
l_totalcont=$1 && shift
l_interestfiles=($@)
r2_1_end=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Reduce\ [tT]ask\ end\ time:\ ([0-9]*) ]]\
      && r2_1_end=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $r2_1_end ]; then
    break
  fi
done
echo $r2_1_end
}

function get_r2_1 {
l_totalcont=$1 && shift
l_interestfiles=($@)
r2_1=""
for i in `seq 0 $((l_totalcont-1))`; do
  #echo ${l_interestfiles[$i]}
  set +x
  while read line; do
    [[ $line =~ $prefix\(2/2\)\ 0\ Reduce\ task\ execution\ time:\ ([0-9]*) ]]\
      && r2_1=${BASH_REMATCH[1]} && break
  done < <(cat ${l_interestfiles[$i]}/stderr)
  set -x
  if [ ! -z $r2_1 ]; then
    break
  fi
done
echo $r2_1
}

function clean {
l_cleanf=$1
if [ $l_cleanf = "yes" ]; then
  if [ ! -z $in_localrepo ]; then
    rm -r -f $in_localrepo
  fi
fi
}

# prepare for main logic
for n in $in_nodes; do
  scp -r -q $in_user@$n:$in_remotehadooplogs $in_localrepo/logs-$n
done
# TODO try to back up the log folder into hadoop-logs in server node

# main logic
mid_maxjobid=0
set +x
for n in $in_nodes; do
  while read name; do
    [[ $name =~ application_[0-9]+_0*([1-9][0-9]*) ]] \
      && jobid=${BASH_REMATCH[1]}
    if [ $jobid -gt $mid_maxjobid ]; then
      mid_maxjobid=$jobid
    fi
  done < <(ls $in_localrepo/logs-$n/$in_userlogs) 
done
set -x
echo mid_maxjobid=$mid_maxjobid

# we omit the last mr2phaseapriori run
# since we don't know whether it's still running
if [ $running = "yes" ]; then
  if [ $((mid_maxjobid%2)) -eq 0 ]; then
    mid_maxjobid=$((mid_maxjobid-2))
  else
    mid_maxjobid=$((mid_maxjobid-1))
  fi
fi

if [ ! -z $executions ]; then
  mid_maxjobid=$((executions*2))
fi


if [ $mid_maxjobid -lt 0 ]; then
  echo Exit, sample space too small
  clean $deb_clean
  exit 1
fi

echo mid_maxjobid=$mid_maxjobid
mid_totalrun=$((mid_maxjobid/2))
echo mid_totalrun=$mid_totalrun

for run in `seq 1 $mid_totalrun`; do
  # for phase 1
  runid=$((run*2-1))
  run1stfolder=

  totalcont=0
  interestfiles=
  for n in $in_nodes; do
    set +x
    while read name; do
      [[ $name =~ (application_[0-9]+_0*$runid) ]] \
        && run1stfolder=${BASH_REMATCH[1]} && break
    done < <(ls $in_localrepo/logs-$n/$in_userlogs) 
    set -x
    run1stfolder=$in_localrepo/logs-$n/$in_userlogs/$run1stfolder

    # stands for total container
    set +x
    while read container; do
      totalcont=$((totalcont+1))
      containerstderr=$run1stfolder/$container
      interestfiles[$totalcont]=$containerstderr
    done < <(ls $run1stfolder)
    set -x

  done

  # now i have phase 1's each task's container stderr
  #for j in `seq 1 $totalcont`; do
  #echo ${interestfiles[$j]}
  #done

  m1_1_start=$(get_m1_1_start $totalcont "${interestfiles[@]}")
  echo -n ${m1_1_start:-NULL}" " >> $user_statpath

  m1_1_end=$(get_m1_1_end $totalcont "${interestfiles[@]}")
  echo -n ${m1_1_end:-NULL}" " >> $user_statpath

  m1_1_loop=$(get_m1_1_loop $totalcont "${interestfiles[@]}")
  echo -n ${m1_1_loop:-NULL}" " >> $user_statpath

  m1_1=$(get_m1_1 $totalcont "${interestfiles[@]}")
  echo -n ${m1_1:-NULL}" " >> $user_statpath

  m1_1_tolerate=$(get_m1_1_tolerate $totalcont "${interestfiles[@]}")
  echo -n ${m1_1_tolerate:-NULL}" " >> $user_statpath

  m1_2_start=$(get_m1_2_start $totalcont "${interestfiles[@]}")
  echo -n ${m1_2_start:-NULL}" " >> $user_statpath

  m1_2_end=$(get_m1_2_end $totalcont "${interestfiles[@]}")
  echo -n ${m1_2_end:-NULL}" " >> $user_statpath

  m1_2_loop=$(get_m1_2_loop $totalcont "${interestfiles[@]}")
  echo -n ${m1_2_loop:-NULL}" " >> $user_statpath

  m1_2=$(get_m1_2 $totalcont "${interestfiles[@]}")
  echo -n ${m1_2:-NULL}" " >> $user_statpath

  m1_2_tolerate=$(get_m1_2_tolerate $totalcont "${interestfiles[@]}")
  echo -n ${m1_2_tolerate:-NULL}" " >> $user_statpath

  r1_1_start=$(get_r1_1_start $totalcont "${interestfiles[@]}")
  echo -n ${r1_1_start:-NULL}" " >> $user_statpath

  r1_1_end=$(get_r1_1_end $totalcont "${interestfiles[@]}")
  echo -n ${r1_1_end:-NULL}" " >> $user_statpath

  r1_1=$(get_r1_1 $totalcont "${interestfiles[@]}")
  echo -n ${r1_1:-NULL}" " >> $user_statpath


  # for phase 2
  runid=$((run*2))
  run2ndfolder=

  totalcont=0
  interestfiles=

  for n in $in_nodes; do
    set +x
    while read name; do
      [[ $name =~ (application_[0-9]+_0*$runid) ]] \
        && run2ndfolder=${BASH_REMATCH[1]} && break
    done < <(ls $in_localrepo/logs-$n/$in_userlogs)
    set -x
    run2ndfolder=$in_localrepo/logs-$n/$in_userlogs/$run2ndfolder

    set +x
    while read container; do
      totalcont=$(($totalcont+1))
      containerstderr=$run2ndfolder/$container
      interestfiles[$totalcont]=$containerstderr
    done < <(ls $run2ndfolder)
    set -x
  done

  # now i have phase 2's each task's container stderr
  #for j in `seq 1 $totalcont`; do
  #echo hahahaha ${interestfiles[$j]}
  #done

  m2_1_start=$(get_m2_1_start $totalcont "${interestfiles[@]}")
  echo -n ${m2_1_start:-NULL}" " >> $user_statpath

  m2_1_end=$(get_m2_1_end $totalcont "${interestfiles[@]}")
  echo -n ${m2_1_end:-NULL}" " >> $user_statpath

  m2_1=$(get_m2_1 $totalcont "${interestfiles[@]}")
  echo -n ${m2_1:-NULL}" " >> $user_statpath

  m2_1_cache_total=$(get_m2_1_cache_total $totalcont "${interestfiles[@]}")
  echo -n ${m2_1_cache_total:-NULL}" " >> $user_statpath

  m2_1_cache_hit=$(get_m2_1_cache_hit $totalcont "${interestfiles[@]}")
  echo -n ${m2_1_cache_hit:-NULL}" " >> $user_statpath

  m2_2_start=$(get_m2_2_start $totalcont "${interestfiles[@]}")
  echo -n ${m2_2_start:-NULL}" " >> $user_statpath

  m2_2_end=$(get_m2_2_end $totalcont "${interestfiles[@]}")
  echo -n ${m2_2_end:-NULL}" " >> $user_statpath

  m2_2=$(get_m2_2 $totalcont "${interestfiles[@]}")
  echo -n ${m2_2:-NULL}" " >> $user_statpath

  m2_2_cache_total=$(get_m2_2_cache_total $totalcont "${interestfiles[@]}")
  echo -n ${m2_2_cache_total:-NULL}" " >> $user_statpath

  m2_2_cache_hit=$(get_m2_2_cache_hit $totalcont "${interestfiles[@]}")
  echo -n ${m2_2_cache_hit:-NULL}" " >> $user_statpath

  r2_1_start=$(get_r2_1_start $totalcont "${interestfiles[@]}")
  echo -n ${r2_1_start:-NULL}" " >> $user_statpath

  r2_1_end=$(get_r2_1_end $totalcont "${interestfiles[@]}")
  echo -n ${r2_1_end:-NULL}" " >> $user_statpath

  r2_1=$(get_r2_1 $totalcont "${interestfiles[@]}")
  echo -n ${r2_1:-NULL}" " >> $user_statpath

  # for t1, t2, t
  echo -n ${tarray[$run]:-NULL}" " >> $user_statpath
  echo -n ${t1array[$run]:-NULL}" " >> $user_statpath
  echo -n ${t2array[$run]:-NULL}" " >> $user_statpath

  # for the newline
  echo >> $user_statpath

done

# clean
#if [ $deb_clean == "yes" ]; then
#  rm -r -f $in_localrepo
#fi
clean $deb_clean

# standard tail
set +x
