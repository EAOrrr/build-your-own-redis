# Build Your Own Redis

该项目是一个基于C++实现的Redis数据库服务器，旨在通过实际开发展示系统编程、数据结构和并发编程等核心技术。项目在[Build Your Own Redis](https://build-your-own.org/redis/)教程的基础上进行了功能扩展和性能优化。

## 支持的命令
- get key
- set key
- del key
- pexpire key ttl_ms
- pttl key
- keys
- zadd zset score name
- zrem zset name
- zscore zset name
- zquery zset score name offset limit
- bgrewriteaof

## 核心功能实现
> 📚 详细的技术文档和学习笔记请参考 [项目学习笔记](./Note.md)

### 网络模块
- 基于事件驱动的I/O多路复用（Event-driven I/O multiplexing）
- 优化的缓冲区设计，提升数据读写效率
- 链表管理的空闲连接池，实现高效的连接复用

### 数据结构
- 高性能哈希表实现，支持动态扩容
- 基于哈希表+AVL树的Sorted Set（Zset）实现
- 小顶堆实现的键值过期管理机制

### 持久化与并发
- AOF（Append Only File）持久化机制
- AOF重写优化，减少磁盘占用
- 线程池实现，提供并发处理能力


## 构建与运行

```bash
# 编译项目
make

# 运行服务器
./redis-server

# 运行客户端
./redis-client [cmds...]
```
