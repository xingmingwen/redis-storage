/*
 * raft_server: 基于 raft.c 的多节点可复制 KV 存储演示。
 *
 * 用法: raft_server <self_idx> <listen_port> <peer1:port,peer2:port,...>
 *   self_idx 为 peer 列表中本节点的下标。
 *
 * 协议（文本，一条消息一行；AE 的日志条目为附加行）：
 *   RV  <term> <candidate> <lastLogIndex> <lastLogTerm>
 *   RVR <term> <granted>
 *   AE  <term> <leader> <prevLogIndex> <prevLogTerm> <leaderCommit> <nentries>
 *   E   <term> <len>\n<data>            (AE 后紧跟 nentries 个)
 *   AER <term> <success> <matchIndex>
 * 客户端:
 *   SET <key> <value>  -> OK / ERR not_leader <leader_idx>
 *   GET <key>          -> value / ERR not_found
 *   STATUS             -> role term commit_index applied leader
 *
 * 线程模型：raft_lock 保护 raft_t；网络发送不在锁内进行 ——
 * raft 回调只把已编码的消息放进对端 outbox，由每个 peer 的 sender
 * 线程异步发出并处理回复（避免持锁网络 IO 造成循环等待死锁）。
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "raft.h"

#define TICK_MS        20
#define NET_TIMEOUT_MS 1500
#define BUFSZ          8192

#define MSG_RV 1
#define MSG_AE 2

typedef struct msg {
    int type;
    int len;
    char *buf;
    struct msg *next;
} msg_t;

typedef struct {
    char host[64];
    int port;
    int idx;
    int fd;                     /* -1 未连接 */
    pthread_mutex_t lock;       /* 保护 outbox 与 fd */
    pthread_cond_t cond;
    msg_t *head, *tail;
} peer_t;

typedef struct {
    raft_t *raft;
    pthread_mutex_t raft_lock;
    peer_t peers[RAFT_MAX_PEERS];
    int npeers;
    int self;

    /* demo 状态机：单链表 KV（访问时持有 raft_lock） */
    struct kv { char *k, *v; struct kv *next; } *kv_head;
} node_t;

static node_t g_node;

/* ---------------- 基础 socket ---------------- */

static int read_line(int fd, char *buf, int cap) {
    int i = 0;
    while (i < cap - 1) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) return -1;
        if (c == '\n') break;
        buf[i++] = c;
    }
    buf[i] = 0;
    return i;
}

static int write_all(int fd, const void *buf, int len) {
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        p += n; len -= n;
    }
    return 0;
}

static int peer_conn(peer_t *p) {
    if (p->fd >= 0) return p->fd;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(p->port);
    inet_pton(AF_INET, p->host, &sa.sin_addr);
    struct timeval tv = { NET_TIMEOUT_MS / 1000,
                          (NET_TIMEOUT_MS % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    p->fd = fd;
    return fd;
}

static void peer_drop(peer_t *p) {
    if (p->fd >= 0) close(p->fd);
    p->fd = -1;
}

/* ---------------- outbox：raft 回调只入队，不阻塞 ---------------- */

static void peer_enqueue(peer_t *p, int type, const char *buf, int len) {
    msg_t *m = malloc(sizeof(msg_t));
    m->type = type;
    m->len = len;
    m->buf = malloc(len);
    memcpy(m->buf, buf, len);
    m->next = NULL;
    pthread_mutex_lock(&p->lock);
    if (p->tail) p->tail->next = m; else p->head = m;
    p->tail = m;
    pthread_cond_signal(&p->cond);
    pthread_mutex_unlock(&p->lock);
}

static msg_t *peer_dequeue(peer_t *p) {
    pthread_mutex_lock(&p->lock);
    while (!p->head)
        pthread_cond_wait(&p->cond, &p->lock);
    msg_t *m = p->head;
    p->head = m->next;
    if (!p->head) p->tail = NULL;
    pthread_mutex_unlock(&p->lock);
    return m;
}

/* sender 线程：串行发送 + 读回复，再持 raft_lock 更新状态 */
static void *peer_sender(void *arg) {
    peer_t *p = arg;
    node_t *n = &g_node;
    char rep[256];

    for (;;) {
        msg_t *m = peer_dequeue(p);

        pthread_mutex_lock(&p->lock);
        int fd = peer_conn(p);
        int ok = 0;
        if (fd >= 0 && write_all(fd, m->buf, m->len) == 0 &&
            read_line(fd, rep, sizeof(rep)) > 0)
            ok = 1;
        if (!ok) peer_drop(p);
        pthread_mutex_unlock(&p->lock);

        if (ok) {
            int a, b, c;
            pthread_mutex_lock(&n->raft_lock);
            if (m->type == MSG_RV) {
                if (sscanf(rep, "RVR %d %d", &a, &b) == 2)
                    raft_on_requestvote_reply(n->raft, p->idx, a, b);
            } else {
                if (sscanf(rep, "AER %d %d %d", &a, &b, &c) == 3)
                    raft_on_appendentries_reply(n->raft, p->idx, a, b, c);
            }
            pthread_mutex_unlock(&n->raft_lock);
        }
        /* 失败则丢弃该消息：心跳/AE 重试由 raft 层驱动，
         * next_index 递减重传已有机制 */
        free(m->buf);
        free(m);
    }
    return NULL;
}

/* ---------------- raft 回调：只编码并入队 ---------------- */

static int cb_send_requestvote(raft_t *r, int idx,
                               int term, int lli, int llt) {
    node_t *n = r->udata;
    char line[256];
    int len = snprintf(line, sizeof(line),
                       "RV %d %d %d %d\n", term, r->self, lli, llt);
    peer_enqueue(&n->peers[idx], MSG_RV, line, len);
    return 0;
}

static int cb_send_appendentries(raft_t *r, int idx,
                                 int term, int pli, int plt,
                                 raft_entry *entries, int nentries,
                                 int leader_commit) {
    node_t *n = r->udata;
    int cap = 512, i;
    for (i = 0; i < nentries; i++) cap += entries[i].len + 64;
    char *buf = malloc(cap), *p = buf;

    p += snprintf(p, cap - (p - buf), "AE %d %d %d %d %d %d\n",
                  term, r->self, pli, plt, leader_commit, nentries);
    for (i = 0; i < nentries; i++) {
        p += snprintf(p, cap - (p - buf), "E %d %d\n",
                      entries[i].term, entries[i].len);
        memcpy(p, entries[i].data, entries[i].len);
        p += entries[i].len;
        *p++ = '\n';
    }
    peer_enqueue(&n->peers[idx], MSG_AE, buf, p - buf);
    free(buf);
    return 0;
}

/* ---------------- demo 状态机：SET k v ---------------- */

static void kv_set(const char *k, const char *v) {
    struct kv *it = g_node.kv_head;
    while (it) {
        if (!strcmp(it->k, k)) {
            free(it->v); it->v = strdup(v); return;
        }
        it = it->next;
    }
    it = malloc(sizeof(*it));
    it->k = strdup(k); it->v = strdup(v);
    it->next = g_node.kv_head; g_node.kv_head = it;
}

static const char *kv_get(const char *k) {
    struct kv *it = g_node.kv_head;
    while (it) {
        if (!strcmp(it->k, k)) return it->v;
        it = it->next;
    }
    return NULL;
}

static void cb_apply(raft_t *r, raft_entry *e) {
    char *copy = strndup(e->data, e->len);
    char k[256], v[4096];
    if (sscanf(copy, "SET %255s %4095[^\n]", k, v) == 2)
        kv_set(k, v);
    free(copy);
    fprintf(stderr, "[node %d] applied (term %d): %.*s\n",
            r->self, e->term, e->len, e->data);
}

/* ---------------- 入站连接处理 ---------------- */

static void handle_rv(int cfd, char *args) {
    int term, cand, lli, llt;
    if (sscanf(args, "%d %d %d %d", &term, &cand, &lli, &llt) != 4) return;
    pthread_mutex_lock(&g_node.raft_lock);
    raft_rv_reply rep = raft_on_requestvote(g_node.raft, term, cand, lli, llt);
    pthread_mutex_unlock(&g_node.raft_lock);
    char out[64];
    int n = snprintf(out, sizeof(out), "RVR %d %d\n", rep.term, rep.granted);
    write_all(cfd, out, n);
}

static void handle_ae(int cfd, char *args) {
    int term, leader, pli, plt, commit, n;
    if (sscanf(args, "%d %d %d %d %d %d",
               &term, &leader, &pli, &plt, &commit, &n) != 6) return;

    raft_entry *entries = calloc(n > 0 ? n : 1, sizeof(raft_entry));
    char hdr[256];
    int i;
    for (i = 0; i < n; i++) {
        int eterm, elen;
        if (read_line(cfd, hdr, sizeof(hdr)) < 0 ||
            sscanf(hdr, "E %d %d", &eterm, &elen) != 2) goto done;
        entries[i].data = malloc(elen + 1);
        int got = 0;
        while (got < elen) {
            ssize_t r = recv(cfd, entries[i].data + got, elen - got, 0);
            if (r <= 0) goto done;
            got += r;
        }
        entries[i].data[elen] = 0;
        char nl;
        recv(cfd, &nl, 1, 0);           /* 吃掉 \n */
        entries[i].term = eterm;
        entries[i].len = elen;
    }

    pthread_mutex_lock(&g_node.raft_lock);
    {
        raft_ae_reply rep = raft_on_appendentries(g_node.raft, term, leader,
                                                  pli, plt, entries, n, commit);
        pthread_mutex_unlock(&g_node.raft_lock);
        char out[64];
        int len = snprintf(out, sizeof(out), "AER %d %d %d\n",
                           rep.term, rep.success, rep.match_index);
        write_all(cfd, out, len);
    }
done:
    for (i = 0; i < n; i++) free(entries[i].data);
    free(entries);
}

static void handle_client(int cfd, char *line) {
    raft_t *r = g_node.raft;
    char out[BUFSZ];

    if (!strncmp(line, "STATUS", 6)) {
        pthread_mutex_lock(&g_node.raft_lock);
        const char *role = r->role == RAFT_LEADER ? "leader" :
                           r->role == RAFT_CANDIDATE ? "candidate" : "follower";
        int n = snprintf(out, sizeof(out),
                         "role=%s term=%d commit=%d applied=%d leader=%d\n",
                         role, r->current_term, r->commit_index,
                         r->last_applied, r->leader_id);
        pthread_mutex_unlock(&g_node.raft_lock);
        write_all(cfd, out, n);
    } else if (!strncmp(line, "GET ", 4)) {
        pthread_mutex_lock(&g_node.raft_lock);
        const char *v = kv_get(line + 4);
        int n = v ? snprintf(out, sizeof(out), "%s\n", v)
                  : snprintf(out, sizeof(out), "ERR not_found\n");
        pthread_mutex_unlock(&g_node.raft_lock);
        write_all(cfd, out, n);
    } else if (!strncmp(line, "SET ", 4)) {
        pthread_mutex_lock(&g_node.raft_lock);
        int rc = raft_propose(r, line, strlen(line));
        int leader = r->leader_id;
        int self = r->self;
        pthread_mutex_unlock(&g_node.raft_lock);
        int n = rc == 0 ? snprintf(out, sizeof(out), "OK\n")
                        : snprintf(out, sizeof(out), "ERR not_leader %d\n",
                                   leader >= 0 ? leader : self);
        write_all(cfd, out, n);
    } else {
        write_all(cfd, "ERR unknown_command\n", 19);
    }
}

static void *conn_thread(void *arg) {
    int cfd = (int)(long)arg;
    struct timeval tv = { 10, 0 };
    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    char line[BUFSZ];
    while (read_line(cfd, line, sizeof(line)) > 0) {
        if (!strncmp(line, "RV ", 3))       handle_rv(cfd, line + 3);
        else if (!strncmp(line, "AE ", 3))  handle_ae(cfd, line + 3);
        else                                handle_client(cfd, line);
    }
    close(cfd);
    return NULL;
}

static void *accept_thread(void *arg) {
    int lfd = (int)(long)arg;
    for (;;) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) continue;
        pthread_t t;
        pthread_create(&t, NULL, conn_thread, (void *)(long)cfd);
        pthread_detach(t);
    }
    return NULL;
}

static void *tick_thread(void *arg) {
    node_t *n = arg;
    for (;;) {
        usleep(TICK_MS * 1000);
        pthread_mutex_lock(&n->raft_lock);
        raft_tick(n->raft, TICK_MS);
        pthread_mutex_unlock(&n->raft_lock);
    }
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr,
                "usage: %s <self_idx> <listen_port> <peer1:port,...,peerN:port>\n",
                argv[0]);
        return 1;
    }
    g_node.self = atoi(argv[1]);
    int port = atoi(argv[2]);

    char *peers = strdup(argv[3]);
    char *tok = strtok(peers, ",:");
    int np = 0;
    while (tok && np < RAFT_MAX_PEERS) {
        snprintf(g_node.peers[np].host, 64, "%s", tok);
        tok = strtok(NULL, ",:");
        g_node.peers[np].port = atoi(tok);
        tok = strtok(NULL, ",:");
        g_node.peers[np].fd = -1;
        g_node.peers[np].idx = np;
        g_node.peers[np].head = g_node.peers[np].tail = NULL;
        pthread_mutex_init(&g_node.peers[np].lock, NULL);
        pthread_cond_init(&g_node.peers[np].cond, NULL);
        pthread_t t;
        pthread_create(&t, NULL, peer_sender, &g_node.peers[np]);
        pthread_detach(t);
        np++;
    }
    g_node.npeers = np;
    pthread_mutex_init(&g_node.raft_lock, NULL);

    raft_cbs cbs = {
        .send_requestvote = cb_send_requestvote,
        .send_appendentries = cb_send_appendentries,
        .apply = cb_apply,
    };
    g_node.raft = raft_new(g_node.self, np, &cbs, &g_node);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port = htons(port);
    if (bind(lfd, (struct sockaddr *)&sa, sizeof(sa)) < 0 ||
        listen(lfd, 64) < 0) {
        perror("bind/listen");
        return 1;
    }

    fprintf(stderr, "[node %d] listening on %d, peers=%d\n",
            g_node.self, port, np);

    pthread_t t1, t2;
    pthread_create(&t1, NULL, accept_thread, (void *)(long)lfd);
    pthread_create(&t2, NULL, tick_thread, &g_node);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    return 0;
}
