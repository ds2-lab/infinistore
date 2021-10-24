deploy:
	aws configure
	aws s3api put-bucket-policy --bucket tianium.default --policy file://${GOPATH}/src/github.com/mason-leap-lab/infinicache/evaluation/cloudwatch/policy.json

prepare:
	mkdir -p bin/

build: prepare
	go build -o bin/proxy ../proxy/

build-local: build
	go build -o bin/lambda ../lambda/

build-data: prepare
	go build -o bin/preprocess benchmark-recovery/preprocess.go

build-bench: prepare
	GO111MODULE=off go build -o bin/redbench github.com/wangaoone/redbench/

microbench:
	./slap.sh 1>./log 2>&1 &

build-simulator: prepare
	GO111MODULE=off go build -o bin/playback github.com/wangaoone/redbench/simulator/playback/

dryrun: build build-simulator
	./playback.sh dryrun /trace/docker_traces/data_centers/csv/dal09_blobs_50h.csv 400 "-d=10 -p=2 -w=100 -balance"

simulate: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_8h.csv 1000 "" "-d=10 -p=2" -compact 1>./log 2>&1 &

simulate-with-dashboard: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_8h.csv 1000 "" "-d=10 -p=2" -compact -enable-dashboard 1>./log 2>&1 &

playback: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h.csv 1000 "" "-d=10 -p=2" 1>./log 2>&1 &

playback-with-dashboard: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h.csv 1000 "" "-d=10 -p=2" -enable-dashboard 1>./log 2>&1 &

playback-static: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h.csv 400 "-cluster=static -functions=400" "-d=10 -p=2 -fo=300" -enable-dashboard 1>./log 2>&1 &

playback-static-average: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h.csv 95 "-cluster=static -functions=95" "-d=10 -p=2 -fo=95" -enable-dashboard 1>./log 2>&1 &

playback-static-norecovery: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h.csv 400 "-cluster=static -functions=400 -disable-recovery" "-d=10 -p=2" 1>./log 2>&1 &

start-server: build
	./server.sh 1>./log 2>&1 &

start-local: build-local
	bin/proxy -cluster=static -functions=10 -invoker=local -disable-recovery -ip=127.0.0.1 $(PARAMS)

stop-server:
	kill -2 $(shell cat /tmp/infinicache.pid)

benchmark: build-bench
	bin/redbench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 0
	bin/redbench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 1 -i 2000

benchmark-recovery: build build-bench
	benchmark-recovery/benchmark.sh 1>./log 2>&1 &

test: build-bench
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -d 10 -p 2 -op 0
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -d 10 -p 2 -op 1

test-redis: build-bench
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -cli redis -op 0
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -cli redis -op 1

test-local: build-bench
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -d 1 -p 0 -op 0
	bin/redbench -n 100 -c 3 -keymin 1 -keymax 1 -d 1 -p 0 -op 1 -i 50

test-local-small: build-bench
	bin/redbench -n 1 -c 1 -keymin 2 -keymax 2 -sz 100 -d 1 -p 0 -op 0
	bin/redbench -n 100 -c 3 -keymin 2 -keymax 2 -d 1 -p 0 -op 1

