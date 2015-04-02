#!/bin/bash
#
#    Copyright (C) 2015 Mesosphere, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

PROJECT_DIR=$(pwd)
TARGET_DIR="cassandra-mesos-dist/target/tarball"
CASSANDRA_VERSION="2.1.4"

DOWNLOAD_URL_CASS="https://downloads.mesosphere.io/cassandra-mesos/cassandra/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"

VERSION=${VERSION:-"dev"}
CLEAN_VERSION=${VERSION//\//_}
PACKAGE_TAR="cassandra-mesos-${CLEAN_VERSION}.tar.gz"

DEPLOY_BUCKET=${DEPLOY_BUCKET:-"downloads.mesosphere.io/cassandra-mesos/artifacts"}/${CLEAN_VERSION}
S3_DEPLOY_BUCKET="s3://${DEPLOY_BUCKET}"
HTTPS_DEPLOY_BUCKET="https://${DEPLOY_BUCKET}"

function _download {
    wget --progress=dot -e dotbytes=1M -O $2 $1
}

function clean {(

    rm -rf ${TARGET_DIR}

)}

function download {(

    if [ ! -f "${TARGET_DIR}/apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz" ] ; then
        mkdir -p ${TARGET_DIR}
        cd ${TARGET_DIR}
        _download ${DOWNLOAD_URL_CASS} "apache-cassandra-${CASSANDRA_VERSION}-bin.tar.gz"
    fi

)}

function copy {(

    mkdir -p ${TARGET_DIR}
    cp ${PROJECT_DIR}/cassandra-mesos-framework/target/cassandra-mesos-framework-*-jar-with-dependencies.jar ${TARGET_DIR}/cassandra-mesos-framework.jar
    cp ${PROJECT_DIR}/cassandra-mesos-executor/target/cassandra-mesos-executor-*-jar-with-dependencies.jar ${TARGET_DIR}/cassandra-mesos-executor.jar
)}


function preparePackage {(

    download
    copy
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
