#ifndef __RAFT_H
#define __RAFT_H

/*
 * Raft 共识协议核心实现。
 * 传输无关：消息的发送通过回调交给上层（raft_net.c），
 * 本模块只负责状态机迁移：选举、日志复制、提交与应用。
 */

#define RAFT_MAX_PEERS 16

typedef enum {
    RAFT_FOLLOWER = 0,
    RAFT_CANDIDATE = 1,
    RAFT_LEADER  = 2
} raft_role;

typedef struct raft_entry {
    int term;           /* 该日志条目的任期 */
    int len;            /* 命令字节数 */
    char *data;         /* 命令内容（状态机负责解释） */
} raft_entry;

typedef struct raft raft_t;

/* 由调用方实现的回调 */
typedef struct raft_cbs {
    /* 向 peer 发送 RequestVote */
    int (*send_requestvote)(raft_t *r, int peer_idx,
                            int term, int last_log_index, int last_log_term);
    /* 向 peer 发送 AppendEntries（entries 为 NULL 时是心跳） */
    int (*send_appendentries)(raft_t *r, int peer_idx,
                              int term, int prev_log_index, int prev_log_term,
                              raft_entry *entries, int nentries, int leader_commit);
    /* 已提交日志应用到状态机 */
    void (*apply)(raft_t *r, raft_entry *e);
} raft_cbs;

struct raft {
    int self;                       /* 本节点在 peers 中的下标 */
    int npeers;                     /* 集群节点总数（含自己） */

    /* ---- 持久状态 ---- */
    int current_term;
    int voted_for;                  /* -1 表示本任期未投票 */
    raft_entry *log;                /* log[0] 是哨兵，index 从 1 开始 */
    int log_len;                    /* 已用条目数 */
    int log_cap;                    /* 容量 */

    /* ---- 易失状态 ---- */
    raft_role role;
    int commit_index;
    int last_applied;
    int leader_id;                  /* -1 未知 */

    /* ---- leader 状态 ---- */
    int next_index[RAFT_MAX_PEERS];
    int match_index[RAFT_MAX_PEERS];

    /* ---- 选举 ---- */
    int votes_received;
    int election_elapsed_ms;        /* 距上次收到心跳/开始选举的毫秒数 */
    int election_timeout_ms;        /* 随机化的选举超时 */
    int heartbeat_interval_ms;

    raft_cbs cbs;
    void *udata;                    /* 上层私有数据 */
};

raft_t *raft_new(int self, int npeers, raft_cbs *cbs, void *udata);
void raft_free(raft_t *r);

/* 每 tick_ms 调用一次，驱动选举超时与心跳 */
void raft_tick(raft_t *r, int tick_ms);

/* 收到对端消息后的处理（返回值忽略，真实回复通过回调或返回值结构传出） */
typedef struct {
    int term;
    int granted;
} raft_rv_reply;

typedef struct {
    int term;
    int success;
    int match_index;    /* 用于推进 next_index */
} raft_ae_reply;

/* 收到 RequestVote 请求 -> 生成回复 */
raft_rv_reply raft_on_requestvote(raft_t *r, int term, int candidate_id,
                                  int last_log_index, int last_log_term);
/* 收到 RequestVote 回复 */
void raft_on_requestvote_reply(raft_t *r, int peer, int term, int granted);
/* 收到 AppendEntries -> 生成回复 */
raft_ae_reply raft_on_appendentries(raft_t *r, int term, int leader_id,
                                    int prev_log_index, int prev_log_term,
                                    raft_entry *entries, int nentries,
                                    int leader_commit);
/* 收到 AppendEntries 回复 */
void raft_on_appendentries_reply(raft_t *r, int peer, int term,
                                 int success, int match_index);

/* 客户端提议一条命令。仅 leader 返回 0，否则返回 -1（调用方应重定向） */
int raft_propose(raft_t *r, const char *data, int len);

int raft_last_log_index(raft_t *r);
int raft_last_log_term(raft_t *r);

#endif
