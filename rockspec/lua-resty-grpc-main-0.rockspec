package = "lua-resty-grpc"
version = "main-0"
source = {
   url = "git+https://github.com/bzp2010/lua-resty-grpc",
   branch = "main",
}

description = {
   summary = "Nonblocking gRPC driver library for OpenResty",
   homepage = "https://github.com/bzp2010/lua-resty-grpc",
   license = "Apache License 2.0",
   maintainer = "Zeping Bai <bzp2010@apache.org>"
}

dependencies = {}

build = {
   type = "make",
   build_variables = {
      CFLAGS="$(CFLAGS)",
      LIBFLAG="$(LIBFLAG)",
      LUA_LIBDIR="$(LUA_LIBDIR)",
      LUA_BINDIR="$(LUA_BINDIR)",
      LUA_INCDIR="$(LUA_INCDIR)",
      LUA="$(LUA)",
   },
   install_variables = {
      INST_PREFIX="$(PREFIX)",
      INST_BINDIR="$(BINDIR)",
      INST_LIBDIR="$(LIBDIR)",
      INST_LUADIR="$(LUADIR)",
      INST_CONFDIR="$(CONFDIR)",
   },
}
