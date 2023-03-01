include config.mk

DATE = $(shell date "+%Y%m%d%H%M")

all: build
	GOMAXPROCS=72 ./bin/proxy -log=proxy.log -enable-dashboard $(PARAMS) 2>error.log

deploy:
	aws configure
	sed 's/@region@/$(REGION)/g;s/@bucket@/$(S3_BUCKET_DATA)/g' cloudwatch/policy.json > cloudwatch/mypolicy.json
	aws s3api put-bucket-policy --bucket $(S3_BUCKET_DATA) --policy file://$(CURDIR)/cloudwatch/mypolicy.json

prepare:
	mkdir -p bin/

prepare-workload-%: $(WORKLOAD_HOME)/%.tar.gz
	$(eval WORKLOAD := $(WORKLOAD_HOME)/$*.csv)
	tar -xzf $(WORKLOAD_HOME)/$*.tar.gz -C $(WORKLOAD_HOME)

# Rename azure workload for simplicity
prepare-workload-azure: prepare-workload-azurefunctions-accesses-2020-bursty

build: prepare
	go build -o bin/proxy ../proxy/

build-local: build
	go build -o bin/lambda ../lambda/

build-data: prepare
	go build -o bin/preprocess preprocess/preprocess.go

build-bench: prepare
	cd $(BENCH_HOME) && go build -o $(CURDIR)/bin/infinibench ./

build-simulator: prepare
	cd $(BENCH_HOME) && go build -o $(CURDIR)/bin/playback ./simulator/playback/

microbench:
	./slap.sh 1>./log 2>&1 &
	@echo "show log: tail -f log"

dryrun: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh dryrun $(WORKLOAD) 1000 "-d=10 -p=2 -w=100 -balance"

simulate: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 1000 "" "-d=10 -p=2" -compact 1>./log 2>&1 &
	@echo "show log: tail -f log"

simulate-with-dashboard: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 1000 "" "-d=10 -p=2" -compact -enable-dashboard 1>./log 2>&1 &
	sleep 1s
	cat ./log
	@echo "show log: tail -f log"

playback: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 1000 "" "-d=10 -p=2" 1>./log 2>&1 &
	@echo "show log: tail -f log"

playback-with-dashboard: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 1000 "" "-d=10 -p=2" -enable-dashboard 1>./log 2>&1 &
	sleep 1s
	cat ./log
	@echo "show log: tail -f log"

playback-azure-with-dashboard: build build-simulator prepare-workload-azure
	./playback.sh playback $(WORKLOAD) 1000 "" "-d=10 -p=2" -enable-dashboard 1>./log 2>&1 &
	sleep 1s
	cat ./log
	@echo "show log: tail -f log"

playback-accelerated: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 1000 "" "-d=10 -p=2 -speed=10" -enable-dashboard 1>./log 2>&1 &
	@echo "show log: tail -f log"

playback-redis: build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh custom $(WORKLOAD) redis '-d 1 -p 0 -c 0 -redis "$(EC_ADDR)" -redisCluster 12' 1>./log 2>&1 &
	@echo "show log: tail -f log"

playback-redis-small: build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh custom $(WORKLOAD) redis '-d 1 -p 0 -c 0 -redis "$(EC_ADDR)" -redisCluster 3 -s3 $(S3_BUCKET_EC_BACKUP) -failover s3' 1>./log 2>&1 &
	@echo "show log: tail -f log"

playback-redis-accelerated: build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh custom $(WORKLOAD) redis '-speed 10 -d 1 -p 0 -c 0 -redis $(EC_ADDR) -redisCluster 8 -s3 $(S3_BUCKET_EC_BACKUP) -failover s3' 1>./log 2>&1 &
	@echo "show log: tail -f log"

playback-static: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 400 "-cluster=static -functions=400" "-d=10 -p=2 -fo=300" -enable-dashboard 1>./log 2>&1 &
	sleep 1s
	cat ./log
	@echo "show log: tail -f log"

playback-static-average: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 95 "-cluster=static -functions=95" "-d=10 -p=2 -fo=300" -enable-dashboard 1>./log 2>&1 &
	sleep 1s
	cat ./log
	@echo "show log: tail -f log"

playback-static-norecovery: build build-simulator prepare-workload-dal09_blobs_50h
	./playback.sh playback $(WORKLOAD) 400 "-cluster=static -functions=400 -disable-recovery" "-d=10 -p=2" -enable-dashboard 1>./log 2>&1 &
	sleep 1s
	cat ./log
	@echo "show log: tail -f log"

start-server: build
	./server.sh 1>./log 2>&1 &

start-local: build-local
	bin/proxy -cluster=static -functions=10 -invoker=local -disable-recovery -ip=127.0.0.1 $(PARAMS)

stop-server:
	kill -2 $(shell cat /tmp/infinistore.pid)

benchmark: build-bench
	bin/infinibench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 0
	bin/infinibench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 1 -i 2000

benchmark-recovery: build build-bench
	benchmark-recovery/benchmark.sh 1>./log 2>&1 &

test: build-bench
	bin/infinibench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -d 10 -p 2 -op 0
	bin/infinibench -n 1 -c 1 -keymin 1 -keymax 1 -d 10 -p 2 -op 1

test-redis: build-bench
	bin/infinibench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -cli redis -op 0
	bin/infinibench -n 1 -c 1 -keymin 1 -keymax 1 -cli redis -op 1

test-local: build-bench
	bin/infinibench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -d 1 -p 0 -op 0
	bin/infinibench -n 1 -c 1 -keymin 1 -keymax 1 -d 1 -p 0 -op 1 -i 50

test-local-small: build-bench
	bin/infinibench -n 1 -c 1 -keymin 2 -keymax 2 -sz 100 -d 1 -p 0 -op 0
	bin/infinibench -n 1 -c 1 -keymin 2 -keymax 2 -d 1 -p 0 -op 1

