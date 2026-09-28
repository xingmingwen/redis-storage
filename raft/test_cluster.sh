#!/bin/bash
# 三节点 raft 集群测试：
#   1. 启动 3 个节点，等待选主
#   2. 向 leader 写入，验证各 follower 已复制
#   3. 杀死 leader，验证重新选主且数据不丢
set -u
cd "$(dirname "$0")"

PORTS="7001 7002 7003"
PEERS="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
PIDS=""
LOGDIR=/tmp/raft_test
mkdir -p $LOGDIR
rm -f $LOGDIR/*.log

cleanup() { [ -n "$PIDS" ] && kill $PIDS 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

say() { echo "== $*"; }
cmd() { nc -w 2 127.0.0.1 $2 <<< "$1" 2>/dev/null; }
leader_port() {
    for p in $PORTS; do
        st=$(cmd STATUS $p)
        case "$st" in *role=leader*) echo $p; return;; esac
    done
}

say "start 3 nodes"
i=0
for p in $PORTS; do
    ./raft_server $i $p "$PEERS" >$LOGDIR/node$i.log 2>&1 &
    PIDS="$PIDS $!"
    i=$((i+1))
done

say "wait for leader election"
L=""
for i in $(seq 1 50); do
    L=$(leader_port); [ -n "$L" ] && break; sleep 0.2
done
[ -z "$L" ] && { echo "FAIL: no leader elected"; exit 1; }
echo "leader: $L"

say "write via leader"
r=$(cmd "SET foo bar" $L)
[ "$r" = "OK" ] || { echo "FAIL: SET -> $r"; exit 1; }

sleep 0.5
say "verify replication on all nodes"
for p in $PORTS; do
    v=$(cmd "GET foo" $p)
    [ "$v" = "bar" ] || { echo "FAIL: GET foo on $p -> $v"; exit 1; }
done
echo "replicated: OK"

say "kill leader $L, expect re-election"
LPID=$(lsof -ti :$L)
kill $LPID 2>/dev/null
NEWL=""
for i in $(seq 1 50); do
    for p in $PORTS; do
        [ "$p" = "$L" ] && continue
        st=$(cmd STATUS $p)
        case "$st" in *role=leader*) NEWL=$p; break;; esac
    done
    [ -n "$NEWL" ] && break
    sleep 0.2
done
[ -z "$NEWL" ] && { echo "FAIL: no new leader"; exit 1; }
echo "new leader: $NEWL"

r=$(cmd "SET baz qux" $NEWL)
[ "$r" = "OK" ] || { echo "FAIL: SET after failover -> $r"; exit 1; }
v=$(cmd "GET baz" $NEWL)
[ "$v" = "qux" ] || { echo "FAIL: GET baz -> $v"; exit 1; }
v=$(cmd "GET foo" $NEWL)
[ "$v" = "bar" ] || { echo "FAIL: foo lost after failover -> $v"; exit 1; }

say "PASS: leader election, replication, and failover all verified"
