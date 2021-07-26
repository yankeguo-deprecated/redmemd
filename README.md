# redmem

以 Redis 为底层，实现 Memcache 协议的主要几个常用命令，为〇山代码提供可持久化的 Memcache 实现

## 依赖

* `redis` > 4

## 启动

**命令行启动**

```shell
# 设置监听端口
export PORT=11211
# 设置 Redis 地址
export REDIS_URL=redis://127.0.0.1:6379/0
# 启动
./redmemed
```

**使用容器**

`guoyk/redmemd`

## 支持的命令

* `version`
* `get`
* `set`, `cas`, `add`, `replace`
* `append`, `prepend`, `incr`, `decr`
* `delete`, `touch`

其中

* 所有命令支持 `flags`, `cas token`, `exptime`, `noreply` 特性
* 所有命令支持原子化操作

## 致谢

Memcache 协议解析代码来自 https://github.com/rpcxio/gomemcached

## 许可证

Guo Y.K., MIT License
