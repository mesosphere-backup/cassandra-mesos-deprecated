#!/bin/bash
set -o errexit -o nounset -o pipefail

PROJECT_DIR=$(pwd)
TARGET_DIR="target/framework-package"

DOWNLOAD_URL_CASS="https://downloads.mesosphere.io/cassandra-mesos/cassandra/apache-cassandra-2.1.2-bin.tar.gz"

VERSION=${VERSION:-"dev"}
PACKAGE_TAR="cassandra-mesos-${VERSION}.tar.gz"

DEPLOY_BUCKET=${DEPLOY_BUCKET:-"downloads.mesosphere.io/cassandra-mesos/artifacts"}/${VERSION}
S3_DEPLOY_BUCKET="s3://${DEPLOY_BUCKET}"
HTTPS_DEPLOY_BUCKET="https://${DEPLOY_BUCKET}"

function _download {
    wget --progress=dot -e dotbytes=1M -O $2 $1
}

function clean {(

    rm -rf ${TARGET_DIR}

)}

function download {(

    mkdir -p ${TARGET_DIR}
    cd ${TARGET_DIR}
    _download ${DOWNLOAD_URL_CASS} "cassandra.tar.gz"

)}

function preparePackage {(

    download

    cp ${PROJECT_DIR}/cassandra-framework/target/cassandra-framework-*-jar-with-dependencies.jar ${TARGET_DIR}/cassandra-framework.jar
    cp ${PROJECT_DIR}/cassandra-executor/target/cassandra-executor-*-jar-with-dependencies.jar ${TARGET_DIR}/cassandra-executor.jar
)}

function package {(

    preparePackage

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
