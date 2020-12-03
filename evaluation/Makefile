deploy:
	aws configure
	aws s3api put-bucket-policy --bucket tianium.default --policy file://${GOPATH}/src/github.com/mason-leap-lab/infinicache/evaluation/cloudwatch/policy.json

prepare:
	mkdir -p bin/

build: prepare
	go build -o bin/proxy ../proxy/

build-data: prepare
	go build -o bin/preprocess benchmark-recovery/preprocess.go

build-bench: prepare
	go build -o bin/redbench github.com/wangaoone/redbench/

microbench:
	./slap.sh 1>./log 2>&1 &

build-simulator: prepare
	go build -o bin/playback github.com/wangaoone/redbench/simulator/playback/

dryrun: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h_fullObj.csv 400 10 2 1 -compact dryrun

simulate: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_8h.csv 400 10 2 1 -compact 1>./log 2>&1 &

simulate-with-dashboard: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_8h.csv 400 10 2 1 -compact -enable-dashboard 1>./log 2>&1 &

playback: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h_fullObj.csv 400 10 2 1 1>./log 2>&1 &

playback-with-dashboard: build build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_blobs_50h_fullObj.csv 400 10 2 1 -enable-dashboard 1>./log 2>&1 &

start-server: build
	./server.sh 1>./log 2>&1 &

stop-server:
	kill -2 $(shell cat /tmp/infinicache.pid)

test: build-bench
	bin/redbench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 0
	bin/redbench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 1 -i 2000

test-redis: build-bench
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -sz 1048576 -cli redis -op 0
	bin/redbench -n 1 -c 1 -keymin 1 -keymax 1 -cli redis -op 1

benchmark: build-bench
	bin/redbench -n 500 -c 1 -keymin 1 -keymax 500 -sz 10000000 -d 10 -p 2 -op 0
	bin/redbench -n 1000 -c 1 -keymin 1 -keymax 500 -sz 10000000 -d 10 -p 2 -op 1 -i 2000

benchmark-recovery: build build-bench
	benchmark-recovery/benchmark.sh 1>./log 2>&1 &
