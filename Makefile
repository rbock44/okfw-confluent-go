GO=GO111MODULE=on go

all: update generate bench test

update:
	cd confluent && $(GO) mod vendor

generate:
	cd confluent && $(GO) generate -mod=vendor

bench: 
	cd confluent && $(GO) test -mod=vendor -bench=.

test:
	cd confluent && $(GO) test

