# Raft 集群模块

基于 Raft 共识协议的复制状态机实现，独立于 redis 主程序，可直接编译运行。

## 结构

- `raft.h` / `raft.c` — 传输无关的 Raft 核心：leader 选举（随机超时）、
  RequestVote / AppendEntries、日志一致性检查与截断、commit_index 推进、
  状态机应用回调。
- `raft_server.c` — TCP 网络层 + 演示用 KV 状态机（`SET`/`GET`/`STATUS`）。
- `test_cluster.sh` — 三节点端到端测试：选主 → 写入复制 → 杀主重选 → 数据校验。

## 构建

```sh
cd raft && make
```

## 运行三节点集群

```sh
./raft_server 0 7001 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 &
./raft_server 1 7002 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 &
./raft_server 2 7003 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 &
```

第一个参数是本节点在 peer 列表中的下标，第二个是监听端口。

## 客户端交互（nc 即可）

```sh
echo "STATUS"   | nc 127.0.0.1 7001   # role/term/commit_index/leader
echo "SET a 1"  | nc 127.0.0.1 7001   # 仅 leader 返回 OK；follower 返回 ERR not_leader <idx>
echo "GET a"    | nc 127.0.0.1 7002
```

## 测试

```sh
./test_cluster.sh
```

## 说明 / 后续工作

- 当前日志与投票状态保存在内存；接入 redis-storage 时需要把
  `current_term`/`voted_for`/log 落盘（可复用 leveldb）。
- `GET` 走本地状态机（follower 可读已提交数据）；强一致读可加
  leader lease 或 read-index。
- AppendEntries 失败回退是逐项递减 next_index，可优化为按 term 跳跃。
