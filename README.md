# Lab 1.MapReduce
### 介绍
简易的分布式并行计算框架，分为`coordinator进程`和`worker进程`。`worker进程`通过RPC与`coordinator`通信，从而获取任务然后并行执行任务。`coordinator进程`负责协调任务的分发。
### 笔记
[Lab 1.MapReduce](https://leggasai.github.io/posts/ddf70eb0/)

# Lab 2.Raft
### 介绍
实现分布式一致性协议Raft，包括领导人选举和心跳机制、日志复制及日志压缩、持久化、快照等。
### 笔记
[Lab 2.Raft](https://leggasai.github.io/posts/afaac1c/)

# Lab 3.KV Raft
### 介绍
基于Raft协议构建可容错的键/值存储服务，包括客户端和服务端部分。该Key/Value服务客户端接收三种基本的操作：`Get`请求、`Put`请求、`Append`请求，客户端通过RPC和服务端进行通信。
### 笔记
[Lab 3.KV Raft](https://leggasai.github.io/posts/82d7eaa9/)

# Lab 4.ShardKV
### 介绍
实现支持分片存储的Key/Value数据库服务，包括`分片服务`和`KV存储服务`。其中`分片服务`负责维护分片配置，并实现分片负载均衡。`KV存储服务`包含 shardkv 服务器，其作为副本组的一部分运行。每个副本组为某些键空间分片提供 `Get`、`Put` 和 `Append` 操作，并需要定时从分片服务中拉取最新分片配置，完成分片数据迁移和回收操作。
### 笔记
[Lab 4.ShardKV](https://leggasai.github.io/posts/ed5ca22/)
