UNAME ?= $(shell uname)
INSTALL ?= install
ECHO ?= echo
C_SO_NAME := librestygrpc.so

ifeq ($(UNAME),Darwin)
	C_SO_NAME := librestygrpc.dylib
endif

all:
	@echo --- Build
	@echo CFLAGS: $(CFLAGS)
	@echo LIBFLAG: $(LIBFLAG)
	@echo LUA_LIBDIR: $(LUA_LIBDIR)
	@echo LUA_BINDIR: $(LUA_BINDIR)
	@echo LUA_INCDIR: $(LUA_INCDIR)

	@echo --- Check Rust toolchain
	@utils/check-rust.sh
	
	@echo --- Build Rust cdylib
	cargo build --release

### install:      Install the library to runtime
.PHONY: install
install:
	@echo --- Install
	@echo INST_PREFIX: $(INST_PREFIX)
	@echo INST_BINDIR: $(INST_BINDIR)
	@echo INST_LIBDIR: $(INST_LIBDIR)
	@echo INST_LUADIR: $(INST_LUADIR)
	@echo INST_CONFDIR: $(INST_CONFDIR)
	$(INSTALL) -d $(INST_LUADIR)/resty/ldap/
	$(INSTALL) lib/resty/*.lua $(INST_LUADIR)/resty/
	$(INSTALL) target/release/${C_SO_NAME} $(INST_LIBDIR)/${C_SO_NAME}

### dev:          Create a development ENV
.PHONY: dev
dev:
	luarocks install rockspec/lua-resty-grpc-main-0.rockspec --only-deps --local

### help:         Show Makefile rules
.PHONY: help
help:
	@echo Makefile rules:
	@echo
	@grep -E '^### [-A-Za-z0-9_]+:' Makefile | sed 's/###/   /'

test:
	prove -r t/
