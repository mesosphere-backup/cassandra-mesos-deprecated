# rest of arguments are arguments to cqlsh/nodetool/cassandra-stress

. `dirname $0`/com-defaults-in.sh

# Locate Cassandra home directory
if [ -z ${CASSANDRA_HOME} ] ; then
    if [ -d apache-cassandra-* ] ; then
        export CASSANDRA_HOME=apache-cassandra-*
    fi
fi

if [ ! -z ${CASSANDRA_HOME} ] ; then
    EXEC=${CASSANDRA_HOME}/${_BINARY}
else
    EXEC=${_BINARY}
fi

if [ ! -x ${EXEC} ] ; then
    echo "Could not locate $EXEC" > /dev/stderr
    exit 1
fi

_QUERY_PARAMS=""
if [ "$1" = "--limit" ] ; then
    shift
    _QUERY_PARAMS="limit=$1"
    shift
fi

API_HOST=${API_HOST:-"127.0.0.1"}
API_PORT=${API_PORT:-18080}
API_BASE_URI=${API_BASE_URI:-"http://$API_HOST:$API_PORT/"}

LIVE_NODES_URI="${API_BASE_URI}live-nodes/${_LIVE_NODES_TYPE}?${_QUERY_PARAMS}"

ARGS=`curl -s ${LIVE_NODES_URI}`
