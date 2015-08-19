#!/bin/bash
# Script to start the job server
set -e

get_abs_script_path() {
  pushd . >/dev/null
  cd $(dirname $0)
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps -Xloggc:$appdir/gc.out
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled"

JAVA_OPTS="-Xmx5g -XX:MaxDirectMemorySize=512M
           -XX:+HeapDumpOnOutOfMemoryError
           -Djava.net.preferIPv4Stack=true
           -Dspray.can.server.parsing.max-content-length=1000m
           -Dspray.can.server.idle-timeout=infinite
           -Dspray.can.server.request-timeout=infinite
           -Dcom.sun.management.jmxremote.port=9999
           -Dcom.sun.management.jmxremote.authenticate=false
           -Dcom.sun.management.jmxremote.ssl=false"

MAIN="spark.jobserver.JobServer"

. $appdir/setenv.sh

if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE"); then
   echo 'Job server is already running'
   exit 1
fi

$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $DRIVER_MEMORY \
    --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS" \
    --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES" \
    $@ $appdir/spark-job-server.jar $conffile &>> $LOG_DIR/jobserver.log