

ROOT != pwd
BIN=cmd/yentablue cmd/ybctl \
	cmd/test_put cmd/test_get cmd/test_conn


GO = env GOBIN=$(ROOT)/bin go


all:
	for x in $(BIN); do \
		( cd $$x; $(GO) install ) ; \
	done

mkproto:
	cd proto;    PATH=$$PATH:$(ROOT)/bin protoc --gofast_out=plugins=grpc:. --proto_path=../src:. *.proto
	cd database; PATH=$$PATH:$(ROOT)/bin protoc --gogoslick_out=. *.proto
	cd merkle;   PATH=$$PATH:$(ROOT)/bin protoc --gogoslick_out=. *.proto
	cd expire;   PATH=$$PATH:$(ROOT)/bin protoc --gogoslick_out=. *.proto
	cd monitor;  PATH=$$PATH:$(ROOT)/bin protoc --gofast_out=. *.proto

mkruby:
	cd proto;    PATH=$$PATH protoc --ruby_out==plugins=grpc:. --proto_path=../src:. *.proto
