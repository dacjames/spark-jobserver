#!/bin/bash
# Script to start the job server
# Extra arguments will be spark-submit options, for example
#  ./server_start.sh --jars cassandra-spark-connector.jar
#
# Environment vars (note settings.sh overrides):
#   JOBSERVER_MEMORY - defaults to 1G, the amount of memory (eg 512m, 2G) to give to job server
#   JOBSERVER_CONFIG - alternate configuration file to use
#   JOBSERVER_FG    - launches job server in foreground; defaults to forking in background
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
         -XX:+CMSClassUnloadingEnabled "

# To truly enable JMX in AWS and other containerized environments, also need to set
# -Djava.rmi.server.hostname equal to the hostname in that environment.  This is specific
# depending on AWS vs GCE etc.
JAVA_OPTS="-XX:MaxDirectMemorySize=512M \
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true \
           -Dcom.sun.management.jmxremote.port=9999 \
           -Dcom.sun.management.jmxremote.rmi.port=9999 \
           -Dcom.sun.management.jmxremote.authenticate=false \
           -Dcom.sun.management.jmxremote.ssl=false"

MAIN="spark.jobserver.JobServer"

. $appdir/setenv.sh

PIDFILE=$appdir/spark-jobserver.pid
if [ -f "$PIDFILE" ] && kill -0 $(cat "$PIDFILE"); then
   echo 'Job server is already running'
   exit 1
fi

exec java -cp $CLASSPATH $GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $MAIN $conffile 2>&1 &
echo $! > $pidFilePath
