#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "raft.h"

#define LOG_GROW 64

static int rand_range(int lo, int hi) {
    return lo + rand() % (hi - lo + 1);
}

static void entry_free(raft_entry *e) {
    free(e->data);
    e->data = NULL;
    e->len = 0;
}

/* log_index 从 1 开始；log[0] 为哨兵（term=0） */
static raft_entry *log_at(raft_t *r, int index) {
    if (index < 0 || index >= r->log_len) return NULL;
    return &r->log[index];
}

int raft_last_log_index(raft_t *r) { return r->log_len - 1; }
int raft_last_log_term(raft_t *r)  { return log_at(r, raft_last_log_index(r))->term; }

static void log_truncate(raft_t *r, int index) {
    int i;
    for (i = index; i < r->log_len; i++) entry_free(&r->log[i]);
    r->log_len = index;
    if (r->commit_index >= r->log_len) r->commit_index = r->log_len - 1;
}

static int log_append(raft_t *r, int term, const char *data, int len) {
    raft_entry *e;
    if (r->log_len == r->log_cap) {
        r->log_cap += LOG_GROW;
        r->log = realloc(r->log, r->log_cap * sizeof(raft_entry));
    }
    e = &r->log[r->log_len++];
    e->term = term;
    e->len = len;
    e->data = malloc(len ? len : 1);
    if (len) memcpy(e->data, data, len);
    return r->log_len - 1;
}

static void reset_election_timeout(raft_t *r) {
    r->election_elapsed_ms = 0;
    /* [heartbeat*5, heartbeat*10] 之间随机，避免选票分裂 */
    r->election_timeout_ms = rand_range(r->heartbeat_interval_ms * 5,
                                        r->heartbeat_interval_ms * 10);
}

static void become_follower(raft_t *r, int term) {
    if (term > r->current_term) {
        r->current_term = term;
        r->voted_for = -1;
    }
    r->role = RAFT_FOLLOWER;
    r->votes_received = 0;
    reset_election_timeout(r);
}

static void become_candidate(raft_t *r) {
    int i;
    r->role = RAFT_CANDIDATE;
    r->current_term++;
    r->voted_for = r->self;
    r->votes_received = 1;
    r->leader_id = -1;
    reset_election_timeout(r);
    for (i = 0; i < r->npeers; i++) {
        if (i == r->self) continue;
        r->cbs.send_requestvote(r, i, r->current_term,
                                raft_last_log_index(r), raft_last_log_term(r));
    }
}

static void broadcast_appendentries(raft_t *r, int peer) {
    int ni = r->next_index[peer];
    int prev_index = ni - 1;
    int prev_term = log_at(r, prev_index)->term;
    int n = r->log_len - ni;
    raft_entry *batch = NULL;

    if (n > 0) {
        int i;
        batch = malloc(n * sizeof(raft_entry));
        for (i = 0; i < n; i++) {
            raft_entry *src = log_at(r, ni + i);
            batch[i].term = src->term;
            batch[i].len = src->len;
            batch[i].data = src->data; /* 发送方只读借用 */
        }
    }
    r->cbs.send_appendentries(r, peer, r->current_term,
                              prev_index, prev_term, batch, n,
                              r->commit_index);
    free(batch);
}

static void broadcast_all(raft_t *r) {
    int i;
    for (i = 0; i < r->npeers; i++)
        if (i != r->self) broadcast_appendentries(r, i);
}

static void become_leader(raft_t *r) {
    int i;
    r->role = RAFT_LEADER;
    r->leader_id = r->self;
    for (i = 0; i < r->npeers; i++) {
        r->next_index[i] = r->log_len;
        r->match_index[i] = 0;
    }
    r->match_index[r->self] = raft_last_log_index(r);
    broadcast_all(r);
}

/* 提交检查：leader 统计 match_index，推进本任期可提交的 commit_index */
static void leader_advance_commit(raft_t *r) {
    int idx;
    for (idx = r->log_len - 1; idx > r->commit_index; idx--) {
        int i, count = 1; /* 自己 */
        if (log_at(r, idx)->term != r->current_term) continue;
        for (i = 0; i < r->npeers; i++)
            if (i != r->self && r->match_index[i] >= idx) count++;
        if (count * 2 > r->npeers) {
            r->commit_index = idx;
            break;
        }
    }
}

static void apply_committed(raft_t *r) {
    while (r->last_applied < r->commit_index) {
        r->last_applied++;
        r->cbs.apply(r, log_at(r, r->last_applied));
    }
}

raft_t *raft_new(int self, int npeers, raft_cbs *cbs, void *udata) {
    raft_t *r = calloc(1, sizeof(raft_t));
    srand(time(NULL) ^ self * 7919);
    r->self = self;
    r->npeers = npeers;
    r->cbs = *cbs;
    r->udata = udata;
    r->voted_for = -1;
    r->leader_id = -1;
    r->heartbeat_interval_ms = 50;
    r->log_cap = LOG_GROW;
    r->log = calloc(r->log_cap, sizeof(raft_entry));
    r->log_len = 1;                 /* 哨兵 */
    r->log[0].term = 0;
    reset_election_timeout(r);
    return r;
}

void raft_free(raft_t *r) {
    int i;
    for (i = 0; i < r->log_len; i++) entry_free(&r->log[i]);
    free(r->log);
    free(r);
}

void raft_tick(raft_t *r, int tick_ms) {
    if (r->role == RAFT_LEADER) {
        r->election_elapsed_ms += tick_ms;
        if (r->election_elapsed_ms >= r->heartbeat_interval_ms) {
            r->election_elapsed_ms = 0;
            broadcast_all(r);
        }
        return;
    }
    r->election_elapsed_ms += tick_ms;
    if (r->election_elapsed_ms >= r->election_timeout_ms) {
        become_candidate(r);
    }
}

raft_rv_reply raft_on_requestvote(raft_t *r, int term, int candidate_id,
                                  int last_log_index, int last_log_term) {
    raft_rv_reply rep = { r->current_term, 0 };
    if (term < r->current_term) return rep;
    if (term > r->current_term) become_follower(r, term);

    /* 候选人日志至少要和自己一样新 */
    int up_to_date = (last_log_term > raft_last_log_term(r)) ||
                     (last_log_term == raft_last_log_term(r) &&
                      last_log_index >= raft_last_log_index(r));
    if ((r->voted_for == -1 || r->voted_for == candidate_id) && up_to_date) {
        r->voted_for = candidate_id;
        rep.granted = 1;
        reset_election_timeout(r);
    }
    rep.term = r->current_term;
    return rep;
}

void raft_on_requestvote_reply(raft_t *r, int peer, int term, int granted) {
    if (term > r->current_term) { become_follower(r, term); return; }
    if (r->role != RAFT_CANDIDATE || term != r->current_term) return;
    if (granted) {
        r->votes_received++;
        if (r->votes_received * 2 > r->npeers) become_leader(r);
    }
}

raft_ae_reply raft_on_appendentries(raft_t *r, int term, int leader_id,
                                    int prev_log_index, int prev_log_term,
                                    raft_entry *entries, int nentries,
                                    int leader_commit) {
    raft_ae_reply rep = { r->current_term, 0, -1 };
    int i, index;

    if (term < r->current_term) return rep;
    if (term >= r->current_term) become_follower(r, term);
    r->leader_id = leader_id;
    reset_election_timeout(r);

    /* 一致性检查 */
    if (prev_log_index >= r->log_len ||
        log_at(r, prev_log_index)->term != prev_log_term) {
        rep.term = r->current_term;
        return rep;
    }

    /* 冲突截断 + 追加 */
    index = prev_log_index + 1;
    for (i = 0; i < nentries; i++, index++) {
        if (index < r->log_len) {
            if (log_at(r, index)->term != entries[i].term)
                log_truncate(r, index);
            else
                continue;               /* 相同条目，跳过 */
        }
        log_append(r, entries[i].term, entries[i].data, entries[i].len);
    }

    if (leader_commit > r->commit_index) {
        int last = r->log_len - 1;
        r->commit_index = leader_commit < last ? leader_commit : last;
    }
    apply_committed(r);

    rep.term = r->current_term;
    rep.success = 1;
    rep.match_index = r->log_len - 1;
    return rep;
}

void raft_on_appendentries_reply(raft_t *r, int peer, int term,
                                 int success, int match_index) {
    if (term > r->current_term) { become_follower(r, term); return; }
    if (r->role != RAFT_LEADER || term != r->current_term) return;

    if (success) {
        if (match_index >= r->match_index[peer]) {
            r->match_index[peer] = match_index;
            r->next_index[peer] = match_index + 1;
        }
        leader_advance_commit(r);
        apply_committed(r);
    } else {
        /* 递减 next_index 重试（简单实现；可优化为按 term 跳跃） */
        if (r->next_index[peer] > 1) r->next_index[peer]--;
        broadcast_appendentries(r, peer);
    }
}

int raft_propose(raft_t *r, const char *data, int len) {
    if (r->role != RAFT_LEADER) return -1;
    log_append(r, r->current_term, data, len);
    r->match_index[r->self] = raft_last_log_index(r);
    leader_advance_commit(r);   /* 单节点集群时立即可提交 */
    apply_committed(r);
    broadcast_all(r);
    return 0;
}
