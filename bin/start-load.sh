#!/bin/bash

#go target dir
bin=`dirname "$0"`
TARGET_HOME=`cd "$bin/../target"; pwd -P`
cd $TARGET_HOME

#pj name
#PJ_NAME="kafka-hadoop-loader-1.0.0-jar-with-dependencies.jar"
PJ_NAME="kafka-hadoop-loader-1.0.0-shaded.jar"

#for remote debug
[ "$1" == "-remote" ] && {
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8000"
echo "remote debug build..."
shift
}||echo "no remote debug"

#need pram
if [ $# -eq 4 ]; then
  ZK=$1
  TOPIC=$2
  GROUP=$3
  HDFS_PATH=$4
  OFFSET="latest"
  Compress="off"
elif [ $# -eq 6 ]; then
  ZK=$1
  TOPIC=$2
  GROUP=$3
  HDFS_PATH=$4
  OFFSET=$5
  Compress=$6
else
  echo "This program needs three arguments. Usage: sh $0 [-remote] <zookeeper> <topic> <group> <hdfsPath> [OFFSET] [Compress]. e.x: sh $0 -remote localhost:2281 test test /tmp latest on"
  exit 1
fi

#hadoop home get
if [ -z "$HADOOP_HOME" ]; then
  echo "Error: HADOOP_HOME is not set and could not be found."
  exit 1
else
  HADOOP="$HADOOP_HOME/bin/hadoop"
fi


SERVICE_CLASS="cn.gitv.bi.k2hloader.start.StartJob"

${HADOOP} jar ${PJ_NAME} ${SERVICE_CLASS} ${ZK} ${TOPIC} ${GROUP} ${HDFS_PATH} ${OFFSET} ${Compress}
