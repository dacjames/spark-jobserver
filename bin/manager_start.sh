#!/bin/bash
# Script to start the job manager
# args: <job manager actor name> <cluster address>
set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd $(dirname $0)
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

. $appdir/setenv.sh

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=512M
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

MAIN="spark.jobserver.JobManager"

. $appdir/setenv.sh

$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $DRIVER_MEMORY \
    --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS" \
    --driver--java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES" \
    ${@:3} $appdir/spark-job-server.jar $1 $2 $conffile &>> $LOG_DIR/manager_start.log &