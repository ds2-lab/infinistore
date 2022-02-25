deploy:
	aws configure

prepare:
	mkdir -p bin/

build: prepare
	go build -o bin/proxy ./proxy/

build-lambda: prepare
	go build -o bin/lambda ./lambda/

build-example: prepare
	go build -o bin/example ./client/example/

start: build build-lambda
	bin/proxy -enable-dashboard $(PARAMS)

start-local: build build-lambda 
	bin/proxy -invoker=local -disable-recovery -ip=127.0.0.1 $(PARAMS)

test: build-example
	bin/example

stop:
	kill -2 $(shell cat /tmp/infinicache.pid)