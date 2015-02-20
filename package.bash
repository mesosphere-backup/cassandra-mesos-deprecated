#!/bin/bash
set -o errexit -o nounset -o pipefail

PROJECT_DIR=$(pwd)
TARGET_DIR="target/framework-package"

VERSION=${VERSION:-"dev"}
CLEAN_VERSION=${VERSION//\//_}
PACKAGE_TAR="cassandra-mesos-${CLEAN_VERSION}.tar.gz"

DEPLOY_BUCKET=${DEPLOY_BUCKET:-"downloads.mesosphere.io/cassandra-mesos/artifacts"}/${CLEAN_VERSION}
S3_DEPLOY_BUCKET="s3://${DEPLOY_BUCKET}"
HTTPS_DEPLOY_BUCKET="https://${DEPLOY_BUCKET}"

function clean {(

    rm -rf ${TARGET_DIR}

)}

function package {(

    mkdir -p ${TARGET_DIR}

    cp ${PROJECT_DIR}/cassandra-framework/target/cassandra-framework-*-jar-with-dependencies.jar ${TARGET_DIR}/cassandra-framework.jar
    cp ${PROJECT_DIR}/cassandra-executor/target/cassandra-executor-*-jar-with-dependencies.jar ${TARGET_DIR}/cassandra-executor.jar

    cd ${TARGET_DIR}
    tar czf ${PACKAGE_TAR} *
    info "Building tar: ${PROJECT_DIR}/${TARGET_DIR}/${PACKAGE_TAR}"

)}

function deploy {(

    package

    local url="${S3_DEPLOY_BUCKET}/${PACKAGE_TAR}"
    info "Uploading tar to: ${url}"
    aws s3 cp ${PROJECT_DIR}/${TARGET_DIR}/${PACKAGE_TAR} ${S3_DEPLOY_BUCKET}/${PACKAGE_TAR}

    info "Generating marathon.json"
    cat ${PROJECT_DIR}/marathon.json | jq ".uris[0] |= \"${HTTPS_DEPLOY_BUCKET}/${PACKAGE_TAR}\"" > ${TARGET_DIR}/marathon.json
    info "marathon.json written ${PROJECT_DIR}/${TARGET_DIR}/marathon.json"

)}

function info { echo "[info] $@" ;}

######################### Delegates to subcommands or runs main, as appropriate
if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else package
fi
