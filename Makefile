BINARY ?= kv-datanode
VERSION ?= v1.7.2
BACKUP_BINARY ?= kv-datanode-backup-launcher
BACKUP_VERSION ?= v1.24.0
IMG ?= harbor.shopeemobile.com/kv/${BINARY}:${VERSION}
# IMG ?= harbor.shopeemobile.com/kv/test/${BINARY}:${VERSION}
BASE_IMAGE ?= harbor.shopeemobile.com/kv/${BACKUP_BINARY}:${BACKUP_VERSION}


TARGETARCH ?= amd64
TARGETOS ?= linux
MORE_BUILD_ARGS ?=

CONTAINER_TOOL ?= docker
IP=$(shell hostname | sed "s/-/./g" | grep -o "[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}")
BASE_DIR=$(shell cd "$(dirname "$0")"; pwd)
TAR=${BINARY}-${VERSION}.tar

JOBS := $(shell nproc)

.PHONY: version-label
version-label:
	$(eval GIT_TAG := $(shell git describe --tags --exact-match 2> /dev/null))
	$(eval GIT_BRANCH_NAME := $(shell git rev-parse --abbrev-ref HEAD))
	$(eval REVISION := $(shell git rev-parse HEAD) $(GIT_TAG) $(GIT_BRANCH_NAME))
	$(eval COMMIT_ID := $(shell git rev-parse HEAD))
	$(eval BUILTAT := $(shell TZ=Asia/Shanghai date +"%Y-%m-%dT%H:%M:%SZ"))

.PHONY: docker-build
docker-build: version-label
	$(CONTAINER_TOOL) build \
		--build-arg BINARY=${BINARY}  \
		--build-arg BACKUP_BINARY=${BACKUP_BINARY}  \
		--build-arg BASE_IMAGE=${BASE_IMAGE}  \
		--build-arg NUM_CORES=${JOBS} \
		--label "revision=${REVISION}" \
		--label "built_at=${BUILTAT}" \
		-t $(IMG) -f Dockerfile .

.PHONY: docker-build-fast
docker-build-fast: version-label
	$(CONTAINER_TOOL) build \
		--build-arg BINARY=${BINARY}  \
		--build-arg BACKUP_BINARY=${BACKUP_BINARY}  \
		--build-arg BASE_IMAGE=${BASE_IMAGE}  \
		--build-arg COMMIT_ID=${COMMIT_ID} \
		--label "revision=${REVISION}" \
		--label "built_at=${BUILTAT}" \
		-t $(IMG) -f Dockerfile.fast .

.PHONY: docker-push
docker-push:
	$(CONTAINER_TOOL) push $(IMG)

.PHONY: build
build:
	echo -n ${VERSION} > src/VERSION.txt
	./x.py build -DENABLE_OPENSSL=OFF \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -j $(JOBS) \
        $(MORE_BUILD_ARGS)

.PHONY: docker-save
docker-save:
	@$(CONTAINER_TOOL) save -o ${TAR} ${IMG}
	@chmod 755 ${TAR}
	@echo "\n=====================================do next steps manually========================================="
	@echo "1.scp image to MAC:"
	@echo "    scp SDE@${IP}:${BASE_DIR}/${TAR} ./${TAR}"
	@echo "2.load image on MAC:"
	@echo "    docker load -i ${TAR}"
	@echo "3.push image on MAC:"
	@echo "    docker push ${IMG}"
