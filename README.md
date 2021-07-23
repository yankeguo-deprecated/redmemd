# redmem

以 Redis 为底层，实现 Memcache 协议的主要几个常用命令，为〇山代码提供可持久化的 Memcache 实现

## 依赖

* `redis` > 6.0，因为需要 keepttl 功能

## 支持的命令

支持 `flags`，支持原子性操作

* `version`
* `set`, `add`, `replace`, `get`, `gets`
* `append`, `prepend`
* `incr`, `decr`
* `delete`, `touch`

## 致谢

Memcache 协议解析代码来自 https://github.com/rpcxio/gomemcached

## 许可证

Guo Y.K., MIT License