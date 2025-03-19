// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>   // isnan
// system
#include <time.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
// C++
#include <string>
#include <vector>
// proj
#include "common.h"
#include "hashtable.h"
#include "zset.h"
#include "list.h"
#include "heap.h"
#include "thread_pool.h"
#include "buffer.h"


static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg) {
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static uint64_t get_monotonic_msec() {
    struct timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000 + tv.tv_nsec / 1000 / 1000;
}

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

const size_t k_max_msg = 32 << 20;  // likely larger than the kernel buffer

// typedef std::vector<uint8_t> Buffer;

// // append to the back
// static void buf_append(Buffer &buf, const uint8_t *data, size_t len) {
//     buf.insert(buf.end(), data, data + len);
// }
// // remove from the front
// static void buf_consume(Buffer &buf, size_t n) {
//     buf.erase(buf.begin(), buf.begin() + n);
// }

struct Conn {
    int fd = -1;
    // application's intention, for the event loop
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    // buffered input and output
    Buffer incoming;    // data to be parsed by the application
    Buffer outgoing;    // responses generated by the application
    // timer
    uint64_t last_active_ms = 0;
    DList idle_node;
};

// global states
static struct {
    HMap db;
    // a map of all client connections, keyed by fd
    std::vector<Conn *> fd2conn;
    // timers for idle connections
    DList idle_list;
    // timers for TTLs
    std::vector<HeapItem> heap;
    // the thread pool
    TheadPool thread_pool;

    // aof related
    int aof_fd = -1;
    uint64_t aof_last_save_ms = 0;
    Buffer aof_buf;
    std::string aof_filename = "redis.aof";
    bool aof_enabled = true;
    // AOF rewrite related
    int aof_rewrite_fd = -1;          // 重写AOF文件的文件描述符
    std::string aof_rewrite_filename; // 重写AOF文件的临时文件名
    bool aof_rewriting = false;       // 是否正在进行AOF重写
    size_t aof_rewrite_progress = 0;  // 重写进度 
} g_data;



// application callback when the listening socket is ready
static int32_t handle_accept(int fd) {
    // accept
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &socklen);
    if (connfd < 0) {
        msg_errno("accept() error");
        return -1;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    // set the new connection fd to nonblocking mode
    fd_set_nb(connfd);

    // create a `struct Conn`
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g_data.idle_list, &conn->idle_node);

    // put it into the map
    if (g_data.fd2conn.size() <= (size_t)conn->fd) {
        g_data.fd2conn.resize(conn->fd + 1);
    }
    assert(!g_data.fd2conn[conn->fd]);
    g_data.fd2conn[conn->fd] = conn;
    return 0;
}

static void conn_destroy(Conn *conn) {
    (void)close(conn->fd);
    g_data.fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);
    delete conn;
}

const size_t k_max_args = 200 * 1000;

static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur + 4 > end) {
        return false;
    }
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

static bool
read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out) {
    if (cur + n > end) {
        return false;
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+

static int32_t
parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr)) {
        return -1;
    }
    if (nstr > k_max_args) {
        return -1;  // safety limit
    }

    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data, end, len)) {
            return -1;
        }
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back())) {
            return -1;
        }
    }
    if (data != end) {
        return -1;  // trailing garbage
    }
    return 0;
}

// error code for TAG_ERR
enum {
    ERR_UNKNOWN = 1,    // unknown command
    ERR_TOO_BIG = 2,    // response too big
    ERR_BAD_TYP = 3,    // unexpected value type
    ERR_BAD_ARG = 4,    // bad arguments
};

// data types of serialized data
enum {
    TAG_NIL = 0,    // nil
    TAG_ERR = 1,    // error code + msg
    TAG_STR = 2,    // string
    TAG_INT = 3,    // int64
    TAG_DBL = 4,    // double
    TAG_ARR = 5,    // array
};

// help functions for the serialization

static void buf_append(Buffer &buf, const uint8_t *data, size_t len) {
    // buf.insert(buf.end(), data, data + len);
    buf.append(data, len);
}

static void buf_append_u8(Buffer &buf, uint8_t data) {
    // buf.push_back(data);
    buf.append_u8(data);
}
static void buf_append_u32(Buffer &buf, uint32_t data) {
    // buf_append(buf, (const uint8_t *)&data, 4);
    buf.append_u32(data);
}
static void buf_append_i64(Buffer &buf, int64_t data) {
    // buf_append(buf, (const uint8_t *)&data, 8);
    buf.append_i64(data);
}
static void buf_append_dbl(Buffer &buf, double data) {
    // buf_append(buf, (const uint8_t *)&data, 8);
    buf.append_dbl(data);
}

// append serialized data types to the back
static void out_nil(Buffer &out) {
    buf_append_u8(out, TAG_NIL);
}
static void out_str(Buffer &out, const char *s, size_t size) {
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)size);
    buf_append(out, (const uint8_t *)s, size);
}
static void out_int(Buffer &out, int64_t val) {
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, val);
}
static void out_dbl(Buffer &out, double val) {
    buf_append_u8(out, TAG_DBL);
    buf_append_dbl(out, val);
}
static void out_err(Buffer &out, uint32_t code, const std::string &msg) {
    buf_append_u8(out, TAG_ERR);
    buf_append_u32(out, code);
    buf_append_u32(out, (uint32_t)msg.size());
    buf_append(out, (const uint8_t *)msg.data(), msg.size());
}
static void out_arr(Buffer &out, uint32_t n) {
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n);
}
static size_t out_begin_arr(Buffer &out) {
    // out.push_back(TAG_ARR);
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, 0);     // filled by out_end_arr()
    return out.size() - 4;      // the `ctx` arg
}
static void out_end_arr(Buffer &out, size_t ctx, uint32_t n) {
    assert(out[ctx - 1] == TAG_ARR);
    memcpy(&out[ctx], &n, 4);
}

// value types
enum {
    T_INIT  = 0,
    T_STR   = 1,    // string
    T_ZSET  = 2,    // sorted set
};

// KV pair for the top-level hashtable
struct Entry {
    struct HNode node;      // hashtable node
    std::string key;
    // for TTL
    size_t heap_idx = -1;   // array index to the heap item
    // value
    uint32_t type = 0;
    // one of the following
    std::string str;
    ZSet zset;
};

static Entry *entry_new(uint32_t type) {
    Entry *ent = new Entry();
    ent->type = type;
    return ent;
}

static void entry_set_ttl(Entry *ent, int64_t ttl_ms);

static void entry_del_sync(Entry *ent) {
    if (ent->type == T_ZSET) {
        zset_clear(&ent->zset);
    }
    delete ent;
}

static void entry_del_func(void *arg) {
    entry_del_sync((Entry *)arg);
}

static void entry_del(Entry *ent) {
    // unlink it from any data structures
    entry_set_ttl(ent, -1); // remove from the heap data structure
    // run the destructor in a thread pool for large data structures
    size_t set_size = (ent->type == T_ZSET) ? hm_size(&ent->zset.hmap) : 0;
    const size_t k_large_container_size = 1000;
    if (set_size > k_large_container_size) {
        thread_pool_queue(&g_data.thread_pool, &entry_del_func, ent);
    } else {
        entry_del_sync(ent);    // small; avoid context switches
    }
}

struct LookupKey {
    struct HNode node;  // hashtable node
    std::string key;
};

// equality comparison for the top-level hashstable
static bool entry_eq(HNode *node, HNode *key) {
    struct Entry *ent = container_of(node, struct Entry, node);
    struct LookupKey *keydata = container_of(key, struct LookupKey, node);
    return ent->key == keydata->key;
}

static void do_get(std::vector<std::string> &cmd, Buffer &out) {
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node) {
        return out_nil(out);
    }
    // copy the value
    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR) {
        return out_err(out, ERR_BAD_TYP, "not a string value");
    }
    return out_str(out, ent->str.data(), ent->str.size());
}

static void do_set(std::vector<std::string> &cmd, Buffer &out) {
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node) {
        // found, update the value
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR) {
            return out_err(out, ERR_BAD_TYP, "a non-string value exists");
        }
        ent->str.swap(cmd[2]);
    } else {
        // not found, allocate & insert a new pair
        Entry *ent = entry_new(T_STR);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->str.swap(cmd[2]);
        hm_insert(&g_data.db, &ent->node);
    }
    return out_nil(out);
}

static void do_del(std::vector<std::string> &cmd, Buffer &out) {
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable delete
    HNode *node = hm_delete(&g_data.db, &key.node, &entry_eq);
    if (node) { // deallocate the pair
        entry_del(container_of(node, Entry, node));
    }
    return out_int(out, node ? 1 : 0);
}

static void heap_delete(std::vector<HeapItem> &a, size_t pos) {
    // swap the erased item with the last item
    a[pos] = a.back();
    a.pop_back();
    // update the swapped item
    if (pos < a.size()) {
        heap_update(a.data(), pos, a.size());
    }
}

static void heap_upsert(std::vector<HeapItem> &a, size_t pos, HeapItem t) {
    if (pos < a.size()) {
        a[pos] = t;         // update an existing item
    } else {
        pos = a.size();
        a.push_back(t);     // or add a new item
    }
    heap_update(a.data(), pos, a.size());
}

// set or remove the TTL
static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1) {
        // setting a negative TTL means removing the TTL
        heap_delete(g_data.heap, ent->heap_idx);
        ent->heap_idx = -1;
    } else if (ttl_ms >= 0) {
        // add or update the heap data structure
        uint64_t expire_at = get_monotonic_msec() + (uint64_t)ttl_ms;
        HeapItem item = {expire_at, &ent->heap_idx};
        heap_upsert(g_data.heap, ent->heap_idx, item);
    }
}

static bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// PEXPIRE key ttl_ms
static void do_expire(std::vector<std::string> &cmd, Buffer &out) {
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2], ttl_ms)) {
        return out_err(out, ERR_BAD_ARG, "expect int64");
    }

    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl_ms);
    }
    return out_int(out, node ? 1: 0);
}

// PTTL key
static void do_ttl(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node) {
        return out_int(out, -2);    // not found
    }

    Entry *ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t)-1) {
        return out_int(out, -1);    // no TTL
    }

    uint64_t expire_at = g_data.heap[ent->heap_idx].val;
    uint64_t now_ms = get_monotonic_msec();
    return out_int(out, expire_at > now_ms ? (expire_at - now_ms) : 0);
}

static bool cb_keys(HNode *node, void *arg) {
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(std::vector<std::string> &, Buffer &out) {
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

static bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd, Buffer &out) {
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_BAD_ARG, "expect float");
    }

    // look up or create the zset
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

    Entry *ent = NULL;
    if (!hnode) {   // insert a new key
        ent = entry_new(T_ZSET);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        hm_insert(&g_data.db, &ent->node);
    } else {        // check the existing key
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET) {
            return out_err(out, ERR_BAD_TYP, "expect zset");
        }
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added = zset_insert(&ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}

static void aof_write_command(Buffer &buf, const std::vector<std::string> &cmd);
static void aof_flush_and_sync();

// 将条目写入AOF重写文件
static void aof_rewrite_entry(Entry *ent, int fd) {
    Buffer buf;
    
    if (ent->type == T_STR) {
        // 为字符串类型构建SET命令
        std::vector<std::string> cmd = {"set", ent->key, ent->str};
        aof_write_command(buf, cmd);
        
        // 如果有TTL，添加PEXPIRE命令
        if (ent->heap_idx != (size_t)-1) {
            int64_t ttl = g_data.heap[ent->heap_idx].val - get_monotonic_msec();
            if (ttl > 0) {
                std::vector<std::string> expire_cmd = {
                    "pexpire", 
                    ent->key, 
                    std::to_string(ttl)
                };
                aof_write_command(buf, expire_cmd);
            }
        }
        
        // 写入文件
        uint8_t *data = NULL;
        size_t data_size;
        buf.get_continuous_data(0, &data, &data_size);
        if (data && data_size > 0) {
            write(fd, data, data_size);
        }
    } else if (ent->type == T_ZSET) {
        // 为有序集合构建ZADD命令
        ZSet *zset = &ent->zset;
        std::string key = ent->key; // 在外部保存key，避免在Lambda中访问可能无效的ent
        
        // 遍历有序集合的所有成员
        struct ForeachData {
            int fd;
            std::string key;
        };
        
        ForeachData foreach_data = {fd, key};
        
        hm_foreach(&zset->hmap, [](HNode *node, void *arg) {
            auto *data = static_cast<ForeachData*>(arg);
            ZNode *znode = container_of(node, ZNode, hmap);
            
            // 安全检查
            if (!znode || znode->len == 0) {
                return true; // 跳过无效的节点
            }
            
            Buffer cmd_buf;
            
            // 安全地创建ZADD命令，确保使用正确的字符串长度
            std::string name(znode->name, znode->len);
            std::vector<std::string> cmd = {
                "zadd", 
                data->key,
                std::to_string(znode->score),
                name
            };
            
            aof_write_command(cmd_buf, cmd);
            
            // 写入文件
            uint8_t *buf_data = NULL;
            size_t data_size;
            cmd_buf.get_continuous_data(0, &buf_data, &data_size);
            if (buf_data && data_size > 0) {
                write(data->fd, buf_data, data_size);
            }
            
            return true;
        }, &foreach_data);
        
        // 如果有TTL，添加PEXPIRE命令
        if (ent->heap_idx != (size_t)-1) {
            Buffer ttl_buf;
            int64_t ttl = g_data.heap[ent->heap_idx].val - get_monotonic_msec();
            if (ttl > 0) {
                std::vector<std::string> expire_cmd = {
                    "pexpire", 
                    key,  // 使用保存的key
                    std::to_string(ttl)
                };
                aof_write_command(ttl_buf, expire_cmd);
                
                // 写入文件
                uint8_t *data = NULL;
                size_t data_size;
                ttl_buf.get_continuous_data(0, &data, &data_size);
                if (data && data_size > 0) {
                    write(fd, data, data_size);
                }
            }
        }
    }
}

// 执行AOF重写
static int32_t aof_rewrite_do() {
    if (!g_data.aof_rewriting || g_data.aof_rewrite_fd < 0) {
        return -1;
    }
    
    msg("Rewriting AOF file...");
    
    // 遍历数据库中的所有条目
    size_t total_entries = hm_size(&g_data.db);
    size_t processed = 0;
    
    hm_foreach(&g_data.db, [](HNode *node, void *arg) {
        Entry *ent = container_of(node, Entry, node);
        int fd = g_data.aof_rewrite_fd;
        
        // 写入条目到AOF重写文件
        aof_rewrite_entry(ent, fd);
        
        return true;
    }, nullptr);
    
    fsync(g_data.aof_rewrite_fd);
    return 0;
}

// 完成AOF重写
static void aof_rewrite_finish() {
    if (!g_data.aof_rewriting) {
        return;
    }
    
    msg("Finishing AOF rewrite...");
    
    // 关闭临时文件
    if (g_data.aof_rewrite_fd >= 0) {
        close(g_data.aof_rewrite_fd);
        g_data.aof_rewrite_fd = -1;
    }
    
    // 确保AOF缓冲区已刷新
    aof_flush_and_sync();
    
    // 使用临时文件替换原AOF文件
    if (rename(g_data.aof_rewrite_filename.c_str(), g_data.aof_filename.c_str()) < 0) {
        msg_errno("rename() error during AOF rewrite");
        unlink(g_data.aof_rewrite_filename.c_str());
        g_data.aof_rewriting = false;
        return;
    }
    
    // 重新打开新的AOF文件
    close(g_data.aof_fd);
    g_data.aof_fd = open(g_data.aof_filename.c_str(), O_WRONLY | O_APPEND, 0644);
    if (g_data.aof_fd < 0) {
        msg_errno("open() error after AOF rewrite");
        g_data.aof_enabled = false;
    } else {
        fd_set_nb(g_data.aof_fd);
    }
    
    g_data.aof_rewriting = false;
    msg("AOF rewrite completed");
}

static int32_t aof_rewrite() {
    if (g_data.aof_rewriting) {
        return -1; // 重写已经在进行中
    }
    msg("AOF rewrite started");
    g_data.aof_rewriting = true;
    g_data.aof_rewrite_progress = 0;
    g_data.aof_rewrite_filename = g_data.aof_filename + ".temp";
    // 创建临时AOF文件
    g_data.aof_rewrite_fd = open(g_data.aof_rewrite_filename.c_str(), 
                                O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (g_data.aof_rewrite_fd < 0) {
        msg_errno("AOF rewrite open() error");
        g_data.aof_rewriting = false;
        return -1;
    }
    
    // 设置为非阻塞模式
    fd_set_nb(g_data.aof_rewrite_fd);
    // 在线程池中执行重写任务
    // thread_pool_queue(&g_data.thread_pool, [](void* arg) {
    //     aof_rewrite_do();
    //     aof_rewrite_finish();
    // }, nullptr);
    aof_rewrite_do();
    aof_rewrite_finish();

    // todo
    return 0;
}


static void do_aof_rewrite(std::vector<std::string> &cmd, Buffer &out) {
    if (!g_data.aof_enabled) {
        return out_err(out, ERR_BAD_ARG, "AOF is not enabled");
    }
    
    if (g_data.aof_rewriting) {
        return out_err(out, ERR_BAD_ARG, "AOF rewrite already in progress");
    }
    
    int32_t rv = aof_rewrite();
    if (rv < 0) {
        return out_err(out, ERR_UNKNOWN, "AOF rewrite failed");
    }
    
    return out_int(out, 1);
}

static const ZSet k_empty_zset;

static ZSet *expect_zset(std::string &s) {
    LookupKey key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode) {   // a non-existent key is treated as an empty zset
        return (ZSet *)&k_empty_zset;
    }
    Entry *ent = container_of(hnode, Entry, node);
    return ent->type == T_ZSET ? &ent->zset : NULL;
}

// zrem zset name
static void do_zrem(std::vector<std::string> &cmd, Buffer &out) {
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset) {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    if (znode) {
        zset_delete(zset, znode);
    }
    return out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(std::vector<std::string> &cmd, Buffer &out) {
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset) {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, Buffer &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return out_err(out, ERR_BAD_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0, limit = 0;
    if (!str2int(cmd[4], offset) || !str2int(cmd[5], limit)) {
        return out_err(out, ERR_BAD_ARG, "expect int");
    }

    // get the zset
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset) {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    // seek to the key
    if (limit <= 0) {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_seekge(zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    // output
    size_t ctx = out_begin_arr(out);
    int64_t n = 0;
    while (znode && n < limit) {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, +1);
        n += 2;
    }
    out_end_arr(out, ctx, (uint32_t)n);
}

static void do_request(std::vector<std::string> &cmd, Buffer &out);

static int32_t load_aof_file() {
    if (!g_data.aof_enabled) {
        return 0;
    }
    bool aof_was_enabled = g_data.aof_enabled;
    g_data.aof_enabled = false;  // disable AOF during loading
    FILE *fp = fopen(g_data.aof_filename.c_str(), "r");
    if (!fp) {
        msg_errno("AOF file not found");
        return 0;
    }

    while (true) {
        std::vector<std::string> cmd;
        uint32_t nstr = 0;
        if (fread(&nstr, 4, 1, fp) != 1) {
            break;
        }
        if (nstr > k_max_args) {
            msg("AOF file is corrupted");
            break;
        }
        for (uint32_t i = 0; i < nstr; ++i) {
            uint32_t len = 0;
            if (fread(&len, 4, 1, fp) != 1) {
                msg("AOF file is corrupted");
                break;
            }
            std::string s(len, '\0');
            if (fread(&s[0], 1, len, fp) != len) {
                msg("AOF file is corrupted");
                break;
            }
            cmd.push_back(std::move(s));
        }
        Buffer out;
        do_request(cmd, out);
    }
    // parse_req
    fclose(fp);
    g_data.aof_enabled = aof_was_enabled;
    return 0;
}

static int32_t aof_init() {
    if (!g_data.aof_enabled) {
        return 0;
    }
    g_data.aof_fd = open(g_data.aof_filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_data.aof_fd < 0) {
        msg_errno("open() error");
        g_data.aof_enabled = false;
        msg("AOF disabled");
        return -1;
    }
    fd_set_nb(g_data.aof_fd);
    msg("AOF enabled");
    if (load_aof_file() < 0) {
        // return -1;
        msg("Warning: AOF file loading failed, continue with empty DB\n");
    }
    return 0;
}

// 格式同输入格式，使写入的 AOF 文件可以直接被加载并被parse_req解析
static void aof_write_command(Buffer &buf, const std::vector<std::string>& cmd) {
    if (cmd.empty()) {
        return;
    }
    
    buf_append_u32(buf, (uint32_t)cmd.size());
    for (const std::string &s : cmd) {
        buf_append_u32(buf, (uint32_t)s.size());
        buf_append(buf, (const uint8_t *)s.data(), s.size());
    }
    
}

static void aof_flush_and_sync() {
    if (!g_data.aof_enabled || g_data.aof_buf.empty() || g_data.aof_fd < 0) {
        return;
    }

    uint8_t *data = NULL;
    size_t data_size;
    g_data.aof_buf.get_continuous_data(0, &data, &data_size);

    ssize_t rv = write(g_data.aof_fd, data, data_size);
    if (rv < 0) {
        msg_errno("write() error");
        return;
    }
    g_data.aof_buf.consume(rv);

    // fsync everysec
    uint64_t now = get_monotonic_msec();
    if (now - g_data.aof_last_save_ms > 1000) {
        fsync(g_data.aof_fd);
        g_data.aof_last_save_ms = now;
    }
}

static void do_request(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        return do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        if (g_data.aof_enabled) aof_write_command(g_data.aof_buf, cmd); // 写入 AOF
        do_set(cmd, out);
        if (g_data.aof_enabled) aof_flush_and_sync();              // 同步 AOF
        return;
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        if (g_data.aof_enabled) aof_write_command(g_data.aof_buf, cmd);
        do_del(cmd, out);
        if (g_data.aof_enabled) aof_flush_and_sync();
        return;
    } else if (cmd.size() == 3 && cmd[0] == "pexpire") {
        if (g_data.aof_enabled) aof_write_command(g_data.aof_buf, cmd);
        do_expire(cmd, out);
        if (g_data.aof_enabled) aof_flush_and_sync();
        return;
    } else if (cmd.size() == 2 && cmd[0] == "pttl") {
        return do_ttl(cmd, out); // pttl 是只读命令，不需要 AOF
    } else if (cmd.size() == 1 && cmd[0] == "keys") {
        return do_keys(cmd, out); // keys 是只读命令
    } else if (cmd.size() == 4 && cmd[0] == "zadd") {
        if (g_data.aof_enabled) aof_write_command(g_data.aof_buf, cmd);
        do_zadd(cmd, out);
        if (g_data.aof_enabled) aof_flush_and_sync();
        return;
    } else if (cmd.size() == 3 && cmd[0] == "zrem") {
        if (g_data.aof_enabled) aof_write_command(g_data.aof_buf, cmd);
        do_zrem(cmd, out);
        if (g_data.aof_enabled) aof_flush_and_sync();
        return;
    } else if (cmd.size() == 3 && cmd[0] == "zscore") {
        return do_zscore(cmd, out); // zscore 是只读命令
    } else if (cmd.size() == 6 && cmd[0] == "zquery") {
        return do_zquery(cmd, out); // zquery 是只读命令
    } else if (cmd.size() == 1 && cmd[0] == "bgrewriteaof") {
        return do_aof_rewrite(cmd, out);

    } else {
        return out_err(out, ERR_UNKNOWN, "unknown command.");
    }
}
/*
static void do_request(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        return do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        return do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        return do_del(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "pexpire") {
        return do_expire(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "pttl") {
        return do_ttl(cmd, out);
    } else if (cmd.size() == 1 && cmd[0] == "keys") {
        return do_keys(cmd, out);
    } else if (cmd.size() == 4 && cmd[0] == "zadd") {
        return do_zadd(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zrem") {
        return do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zscore") {
        return do_zscore(cmd, out);
    } else if (cmd.size() == 6 && cmd[0] == "zquery") {
        return do_zquery(cmd, out);
    } else {
        return out_err(out, ERR_UNKNOWN, "unknown command.");
    }
}
*/

static void response_begin(Buffer &out, size_t *header) {
    *header = out.size();       // messege header position
    buf_append_u32(out, 0);     // reserve space
}
static size_t response_size(Buffer &out, size_t header) {
    return out.size() - header - 4;
}
static void response_end(Buffer &out, size_t header) {
    size_t msg_size = response_size(out, header);
    if (msg_size > k_max_msg) {
        out.resize(header + 4);
        out_err(out, ERR_TOO_BIG, "response is too big.");
        msg_size = response_size(out, header);
    }
    // message header
    uint32_t len = (uint32_t)msg_size;
    // memcpy(&out[header], &len, 4);
    out.insert((uint8_t *)&len, 4, header);
    
    
}


// process 1 request if there is enough data
static bool try_one_request(Conn *conn) {
    // try to parse the protocol: message header
    if (conn->incoming.size() < 4) {
        return false;   // want read
    }
    uint32_t len = conn->incoming.peek_u32(0);
    // memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) {
        msg("too long");
        conn->want_close = true;
        return false;   // want close
    }
    // message body
    if (4 + len > conn->incoming.size()) {
        return false;   // want read
    }
    // const uint8_t *request = &conn->incoming[4];
    // 获取请求数据
    uint8_t *request = new uint8_t[len];
    conn->incoming.peek(request, 4, len);

    // got one request, do some application logic
    std::vector<std::string> cmd;
    if (parse_req(request, len, cmd) < 0) {
        delete[] request;
        msg("bad request");
        conn->want_close = true;
        return false;   // want close
    }
    size_t header_pos = 0;
    response_begin(conn->outgoing, &header_pos);
    do_request(cmd, conn->outgoing);
    response_end(conn->outgoing, header_pos);
    delete[] request;

    // application logic done! remove the request message.
    // buf_consume(conn->incoming, 4 + len);
    conn->incoming.consume(4 + len);
    // Q: Why not just empty the buffer? See the explanation of "pipelining".
    return true;        // success
}

// application callback when the socket is writable
static void handle_write(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    // 获取连续的数据块用于写入
    uint8_t *data_ptr;
    size_t data_size;
    conn->outgoing.get_continuous_data(0, &data_ptr, &data_size);
    
    ssize_t rv = write(conn->fd, data_ptr, data_size);
    // ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return; // actually not ready
    }
    if (rv < 0) {
        msg_errno("write() error");
        conn->want_close = true;    // error handling
        return;
    }

    // remove written data from `outgoing`
    // buf_consume(conn->outgoing, (size_t)rv);
    conn->outgoing.consume((size_t)rv);

    // update the readiness intention
    if (conn->outgoing.size() == 0) {   // all data written
        conn->want_read = true;
        conn->want_write = false;
    } // else: want write
}

// application callback when the socket is readable
static void handle_read(Conn *conn) {
    // read some data
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) {
        return; // actually not ready
    }
    // handle IO error
    if (rv < 0) {
        msg_errno("read() error");
        conn->want_close = true;
        return; // want close
    }
    // handle EOF
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return; // want close
    }
    // got some new data
    buf_append(conn->incoming, buf, (size_t)rv);

    // parse requests and generate responses
    while (try_one_request(conn)) {}
    // Q: Why calling this in a loop? See the explanation of "pipelining".

    // update the readiness intention
    if (conn->outgoing.size() > 0) {    // has a response
        conn->want_read = false;
        conn->want_write = true;
        // The socket is likely ready to write in a request-response protocol,
        // try to write it without waiting for the next iteration.
        return handle_write(conn);
    }   // else: want read
}

const uint64_t k_idle_timeout_ms = 5 * 1000;

static uint32_t next_timer_ms() {
    uint64_t now_ms = get_monotonic_msec();
    uint64_t next_ms = (uint64_t)-1;
    // idle timers using a linked list
    if (!dlist_empty(&g_data.idle_list)) {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        next_ms = conn->last_active_ms + k_idle_timeout_ms;
    }
    // TTL timers using a heap
    if (!g_data.heap.empty() && g_data.heap[0].val < next_ms) {
        next_ms = g_data.heap[0].val;
    }
    // timeout value
    if (next_ms == (uint64_t)-1) {
        return -1;  // no timers, no timeouts
    }
    if (next_ms <= now_ms) {
        return 0;   // missed?
    }
    return (int32_t)(next_ms - now_ms);
}

static bool hnode_same(HNode *node, HNode *key) {
    return node == key;
}

static void process_timers() {
    uint64_t now_ms = get_monotonic_msec();
    // idle timers using a linked list
    while (!dlist_empty(&g_data.idle_list)) {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
        if (next_ms >= now_ms) {
            break;  // not expired
        }

        fprintf(stderr, "removing idle connection: %d\n", conn->fd);
        conn_destroy(conn);
    }
    // TTL timers using a heap
    const size_t k_max_works = 2000;
    size_t nworks = 0;
    const std::vector<HeapItem> &heap = g_data.heap;
    while (!heap.empty() && heap[0].val < now_ms) {
        Entry *ent = container_of(heap[0].ref, Entry, heap_idx);
        HNode *node = hm_delete(&g_data.db, &ent->node, &hnode_same);
        assert(node == &ent->node);
        // fprintf(stderr, "key expired: %s\n", ent->key.c_str());
        // delete the key
        entry_del(ent);
        if (nworks++ >= k_max_works) {
            // don't stall the server if too many keys are expiring at once
            break;
        }
    }
}

int main() {
    // initialization
    dlist_init(&g_data.idle_list);
    thread_pool_init(&g_data.thread_pool, 4);
    aof_init();

    // the listening socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);    // wildcard address 0.0.0.0
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }

    // set the listen fd to nonblocking mode
    fd_set_nb(fd);

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    // the event loop
    std::vector<struct pollfd> poll_args;
    while (true) {
        // prepare the arguments of the poll()
        poll_args.clear();
        // put the listening sockets in the first position
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);
        // the rest are connection sockets
        for (Conn *conn : g_data.fd2conn) {
            if (!conn) {
                continue;
            }
            // always poll() for error
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            // poll() flags from the application's intent
            if (conn->want_read) {
                pfd.events |= POLLIN;
            }
            if (conn->want_write) {
                pfd.events |= POLLOUT;
            }
            poll_args.push_back(pfd);
        }

        // wait for readiness
        int32_t timeout_ms = next_timer_ms();
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout_ms);
        if (rv < 0 && errno == EINTR) {
            continue;   // not an error
        }
        if (rv < 0) {
            die("poll");
        }

        // handle the listening socket
        if (poll_args[0].revents) {
            handle_accept(fd);
        }

        // handle connection sockets
        for (size_t i = 1; i < poll_args.size(); ++i) { // note: skip the 1st
            uint32_t ready = poll_args[i].revents;
            if (ready == 0) {
                continue;
            }
            Conn *conn = g_data.fd2conn[poll_args[i].fd];

            // update the idle timer by moving conn to the end of the list
            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_data.idle_list, &conn->idle_node);

            // handle IO
            if (ready & POLLIN) {
                assert(conn->want_read);
                handle_read(conn);  // application logic
            }
            if (ready & POLLOUT) {
                assert(conn->want_write);
                handle_write(conn); // application logic
            }

            // close the socket from socket error or application logic
            if ((ready & POLLERR) || conn->want_close) {
                conn_destroy(conn);
            }
        }   // for each connection sockets

        // handle timers
        process_timers();
    }   // the event loop
    if (g_data.aof_fd != -1) {
        close(g_data.aof_fd);
        g_data.aof_fd = -1;
        fprintf(stderr, "AOF file closed.\n");
    }
    return 0;
}
