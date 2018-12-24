

ROOT != pwd
BIN=src/cmd/yentablue src/cmd/ybctl \
	src/cmd/test_put src/cmd/test_get src/cmd/test_conn


GO=env GOPATH=$(ROOT) go


all: src/.deps
	for x in $(BIN); do \
		( cd $$x; $(GO) install ) ; \
	done

src/.deps: deps
	for d in `cat deps`; do \
	        $(GO) get -insecure $$d; \
	done
	-ln -s $(ROOT) src/github.com/jaw0/yentablue
	touch src/.deps

mkproto:
	cd proto;    PATH=$$PATH:$(ROOT)/bin protoc --gofast_out=plugins=grpc:. --proto_path=../src:. *.proto
	cd database; PATH=$$PATH:$(ROOT)/bin protoc --gogoslick_out=. *.proto
	cd merkle;   PATH=$$PATH:$(ROOT)/bin protoc --gogoslick_out=. *.proto
	cd expire;   PATH=$$PATH:$(ROOT)/bin protoc --gogoslick_out=. *.proto
	cd monitor;  PATH=$$PATH:$(ROOT)/bin protoc --gofast_out=. *.proto

