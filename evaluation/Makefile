deploy:
	aws configure
	aws s3api put-bucket-policy --bucket tianium.default --policy file://${GOPATH}/src/github.com/mason-leap-lab/infinicache/evaluation/cloudwatch/policy.json

prepare:
	mkdir -p bin/

build: prepare
	go build -o bin/proxy ../proxy/
	go build -o bin/preprocess benchmark-recovery/preprocess.go

build-bench: prepare
	go build -o bin/redbench ${GOPATH}/src/github.com/wangaoone/redbench/

microbench:
	./slap.sh 1>./log 2>&1 &

build-simulator:
	go build ${GOPATH}/src/github.com/wangaoone/redbench/simulator/playback/
	mv playback ${GOPATH}/src/github.com/wangaoone/redbench/simulator/playback/

dryrun: build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_50h.csv 400 10 2 0.1 -compact dryrun

simulate: build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_50h.csv 400 10 2 0.1 -compact 1>./log 2>&1 &

playback: build-simulator
	./playback.sh /trace/docker_traces/data_centers/csv/dal09_10mb_50h.csv 400 10 2 0.1 1>./log 2>&1 &

start-server:
	./server.sh 1>./log 2>&1 &

stop-server:
	kill -2 $(shell cat /tmp/infinicache.pid)

benchmark:
	go run ${GOPATH}/src/github.com/wangaoone/redbench/bench.go -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 0
	go run ${GOPATH}/src/github.com/wangaoone/redbench/bench.go -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 1 -i 2000

benchmark-recovery: build build-bench
	benchmark-recovery/benchmark.sh 1>./log 2>&1 &
