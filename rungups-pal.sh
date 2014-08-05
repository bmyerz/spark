#!/bin/bash

GUPS_HOSTS=$1
taskspernode=`echo $SLURM_TASKS_PER_NODE | cut -d '(' -f 1`
echo "taskspernode: $taskspernode"
localid=$(( $SLURM_NODEID * $taskspernode + $SLURM_LOCALID ))
echo "localid: $localid"

java -cp core/target/scala-2.10/classes/:`$SPARK_v1/bin/compute-classpath.sh` \
    org.apache.spark.network.Gups \
    $localid \
    ~/escience/spark/hosts \
    $taskspernode \
    1000000 \
    $(( 10000000 * $taskspernode * $SLURM_NNODES )) \
    6140 \

# usage: <myid> <hostsfile> <ppn> <numupdates> <Asize> <baseport>
