# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
ARG BINARY
ARG BACKUP_BINARY
ARG BASE_IMAGE
ARG NUM_CORES=$(nproc)

FROM alpine:3.16 as build
ARG MORE_BUILD_ARGS

RUN apk update \
    && apk add \
    git \
    gcc \
    g++ \
    make \
    cmake \
    ninja \
    autoconf \
    automake \
    libtool \
    python3 \
    linux-headers \
    curl \
    openssl-dev \
    libexecinfo-dev \
    # benchmark \
    # benchmark-dev \
    redis

WORKDIR /datanode

COPY . .

RUN whoami
RUN pwd
RUN ls
RUN umask
RUN umask 002 && mkdir -p /datanode/build/cmake_tmp
RUN ls -ld /datanode /datanode/build
RUN ls -ld /usr/bin/cmake
ENV TMPDIR=/datanode/build/cmake_tmp
RUN mkdir -p /datanode/build/cmake_tmp && chmod 777 /datanode/build/cmake_tmp
RUN ls -ld /usr/bin/make

RUN ./x.py build \
        -DENABLE_OPENSSL=OFF \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -j "${NUM_CORES}" \
        $MORE_BUILD_ARGS

FROM $BASE_IMAGE

WORKDIR /

# grab gosu for easy step-down from root
# https://github.com/tianon/gosu/releases
ENV GOSU_VERSION 1.17
RUN set -eux; \
	apk add --no-cache --virtual .gosu-fetch gnupg; \
	arch="$(apk --print-arch)"; \
	case "$arch" in \
		'x86_64') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-amd64'; sha256='bbc4136d03ab138b1ad66fa4fc051bafc6cc7ffae632b069a53657279a450de3' ;; \
		'aarch64') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-arm64'; sha256='c3805a85d17f4454c23d7059bcb97e1ec1af272b90126e79ed002342de08389b' ;; \
		'armhf') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-armhf'; sha256='e5866286277ff2a2159fb9196fea13e0a59d3f1091ea46ddb985160b94b6841b' ;; \
		'x86') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-i386'; sha256='087dbb8fe479537e64f9c86fa49ff3b41dee1cbd28739a19aaef83dc8186b1ca' ;; \
		'ppc64le') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-ppc64el'; sha256='1891acdcfa70046818ab6ed3c52b9d42fa10fbb7b340eb429c8c7849691dbd76' ;; \
		'riscv64') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-riscv64'; sha256='38a6444b57adce135c42d5a3689f616fc7803ddc7a07ff6f946f2ebc67a26ba6' ;; \
		's390x') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-s390x'; sha256='69873bab588192f760547ca1f75b27cfcf106e9f7403fee6fd0600bc914979d0' ;; \
		'armv7') url='https://github.com/tianon/gosu/releases/download/1.17/gosu-armhf'; sha256='e5866286277ff2a2159fb9196fea13e0a59d3f1091ea46ddb985160b94b6841b' ;; \
		*) echo >&2 "error: unsupported gosu architecture: '$arch'"; exit 1 ;; \
	esac; \
	wget -O /usr/local/bin/gosu.asc "$url.asc"; \
	wget -O /usr/local/bin/gosu "$url"; \
	echo "$sha256 */usr/local/bin/gosu" | sha256sum -c -; \
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
	gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
	apk del --no-network .gosu-fetch; \
	chmod +x /usr/local/bin/gosu; \
	gosu --version; \
	gosu nobody true

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN set -eux; \
        # alpine already has a gid 999, so we'll use the next id
        if ! getent group kv >/dev/null; then \
            addgroup -S -g 1000 kv; \
        fi; \
        if ! getent passwd kv >/dev/null; then \
            adduser -S -G kv -u 999 kv; \
        fi; \
        echo -e '#!/bin/sh\n\n\
set -e\n\n\
# allow the container to be started with `--user`\n\
if [ "$(id -u)" = "0" ]; then\n\
    if [ "$1" = "'/usr/local/bin/${BINARY:-kv-datanode}'" ] || [ "$1" = "'${BINARY:-kv-datanode}'" ] \
    || [ "$1" = "'/usr/local/bin/${BACKUP_BINARY:-kv-datanode-backup-launcher}'" ] || [ "$1" = "'${BACKUP_BINARY:-kv-datanode-backup-launcher}'" ] ; then\n\
        exec gosu kv "$0" "$@"\n\
    fi\n\
fi\n\n\
# set an appropriate umask (if one isn"t set already)\n\
um="$(umask)"\n\
if [ "$um" = "0022" ]; then\n\
umask 0077\n\
fi\n\n\
exec "$@"' > /usr/local/bin/docker-entrypoint.sh &&\
        chmod +x /usr/local/bin/docker-entrypoint.sh &&    \
        mkdir -p /data/${BINARY:-kv-datanode} /data/log/${BINARY:-kv-datanode} && \
        chown kv:kv -R /data

VOLUME [ "/data" ]

RUN apk add --no-cache su-exec tzdata \
	&& cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
	&& echo "Asia/Shanghai" > /etc/timezone \
	&& apk del tzdata \
    && apk add libexecinfo

COPY --from=build /datanode/build/datanode /usr/local/bin/${BINARY:-kv-datanode}
COPY --from=build /usr/bin/redis-cli /usr/local/bin/

COPY ./LICENSE ./NOTICE ./licenses /doc/


ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
