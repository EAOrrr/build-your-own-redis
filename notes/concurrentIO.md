# 并发IO模型
## 1. 基于线程的并发
- **原理**：为每个新的客户端连接创建一个新的线程来处理请求。这种模型简单直观，每个连接都有独立的执行上下文。
- **伪代码**：
	```py
	fd = socket()
	bind(fd, address)
	listen(fd)

	while True:
	    conn_fd = accept(fd)
	    new_thread(do_something_with, conn_fd)
	    # 继续接受下一个客户端连接，不会阻塞
	```
- **缺点**：
    1. 内存占用：大量的线程意味着需要大量的栈空间，这会导致内存消耗难以控制。尤其在高并发场景下，内存压力会非常显著。
    2. 开销：对于短连接的应用场景（例如 PHP 应用），频繁地创建和销毁线程会带来额外的延迟和 CPU 消耗。线程的创建和销毁本身就是比较重的操作。
- **适用场景**：连接数量不多，易于实现，容错性高。由于实现简单，并且每个连接隔离性好，即使某个线程崩溃，也不会影响其他连接。

## 2. 基于事件的并发
### 2.1 简介
- **核心思想**：单线程处理多个连接。通过监听 socket 事件（例如可读、可写事件）来避免线程阻塞，从而在一个线程内高效地处理多个并发连接。
- **关键机制**：
    - 就绪通知 (Readiness notification)：使用 `wait_for_readiness()` 等待多个 socket。当一个或多个 socket 准备好进行读/写操作时，该函数会返回，告知哪些 socket 可以进行下一步操作。
    - 非阻塞读 (Non-blocking read)：使用 `read_nb()` 尝试从内核缓冲区读取数据。如果缓冲区为空，`read_nb()` 会立即返回 `EAGAIN` 错误，而不会阻塞线程。程序需要处理 `EAGAIN` 并稍后重试。
    - 非阻塞写 (Non-blocking write)：使用 `write_nb()` 尝试将数据写入内核缓冲区。如果缓冲区已满，`write_nb()` 会立即返回 `EAGAIN` 错误，不会阻塞线程。程序同样需要处理 `EAGAIN` 并稍后重试。
- **伪代码**：
```py
while running:
    want_read = [...]           # socket fds
    want_write = [...]          # socket fds
    can_read, can_write = wait_for_readiness(want_read, want_write) # blocks!
    for fd in can_read:
        data = read_nb(fd)      # non-blocking, only consume from the buffer
        handle_data(fd, data)   # application logic without IO
    for fd in can_write:
        data = pending_data(fd) # produced by the application
        n = write_nb(fd, data)  # non-blocking, only append to the buffer
        data_written(fd, n)     # n <= len(data), limited by the available space
```
**回调编程 (Callback-based programming)**：事件循环通常与回调函数结合使用。当监听的事件发生时，事件循环会调用预先注册好的回调函数来处理相应的事件，使得程序结构更清晰，逻辑更模块化。
### 2.2 Readiness API
| API  | 特点 | 平台       |
|---|----|------|
| `poll()`     | Linux 上最简单的就绪 API。使用 `pollfd` 结构，可以监控多个文件描述符的读写就绪状态。                                                                                               | Linux      |
| `select()`   | 类似于 `poll()`，但存在于 Windows 和 Unix 平台。缺点是只能处理最多 1024 个文件描述符，因此在高并发场景下不推荐使用。                                                                        | Windows, Unix |
| `epoll_wait()` | Linux 独有的就绪 API。相比 `poll()`，它更具可扩展性，因为文件描述符列表存储在内核中，避免了每次调用时传递列表的开销。使用 `epoll_ctl()` 管理文件描述符列表。在 Linux 上是更高效和推荐的选择。 | Linux      |
| `kqueue()`   | BSD 独有的就绪 API。类似于 `epoll()`，但可以通过批量更新文件描述符列表来减少系统调用次数，从而更加高效。                                                                                | BSD        |

> 注意：Readiness APIs 不能用于文件：当套接字准备好读取时，意味着数据已经在读取缓冲区中，因此可以保证读取操作不会阻塞。但是对于磁盘文件，内核中不存在这样的缓冲区，因此磁盘文件的就绪状态是未定义的。这些 API 总是会将磁盘文件报告为就绪状态，但实际的 IO 操作仍然可能会阻塞。
### 2.3 具体代码
完整的具体代码 可以参考 [https://build-your-own.org/redis/06_event_loop_impl](https://build-your-own.org/redis/06_event_loop_impl)。
以下是个人理解的代码包含的步骤：
- 事件循环步骤：
    1. 构建 `poll()` 参数 (`poll_args`)
    2. 调用 `poll()`：
    3. 处理新连接，检查`listening socket`状态
    4. 调用应用回调：
        - 如果 `ready & POLLIN`，表示 socket 可读，调用 `handle_read(conn)` 处理读事件，从 socket 读取数据。
        - 如果 `ready & POLLOUT`，表示 socket 可写，调用 `handle_write(conn)` 处理写事件，向 socket 写入数据。
    5. 终止连接 (Terminate connections)：
        - 在处理连接 socket 就绪状态的循环中，检查 `ready & POLLERR` 或者 `conn->want_close` 标志。`POLLERR` 表示 socket 发生错误，`conn->want_close` 表示连接需要主动关闭（例如，请求处理出错或客户端主动断开）。
- 非阻塞 `accept()` 实现 (`handle_accept()`)：
    - 调用 `accept()` 接受新的连接。由于监听 socket 是非阻塞的，`accept()` 本身是非阻塞的。
    - 使用 `fd_set_nb()` 设置新连接 socket 为非阻塞模式。
    - 创建新的 `Conn` 对象，用于管理这个新的连接。
- 协议解析与非阻塞读：
    - `handle_read(Conn *conn)` 步骤：
        1. 执行非阻塞读取。
        2. 将新数据添加到 `Conn::incoming` 缓冲区。
        3. 尝试解析累积的缓冲区。
        4. 处理解析的消息。
        5. 从 `Conn::incoming` 中删除消息。
        （其中3、4、5步交给 `try_one_request`）
        6. 更新链接读写状态。   

- 非阻塞写 (`handle_write()`)：
    1. 从 `conn->outgoing` 缓冲区读取数据，并使用非阻塞 `write()` 系统调用写入 socket
    2. 从移除 `conn->outgoing` 中移除实际写入的数据量。
    3. 更新链接读写状态 

- 请求和响应之间的状态转换 (`handle_read()` 和 `handle_write()` 中的状态更新)：
    - `handle_read()`：如果在 `handle_read()` 中成功生成了响应数据（即 `conn->outgoing.size() > 0`），则需要更新连接的状态，准备进行写操作。
    - `handle_write()`：当所有响应数据都已写入 socket（即 `conn->outgoing.size() == 0`），表示写操作完成，需要切换回读状态，等待接收新的请求。

### 2.4 小技巧
#### 2.4.1. 请求流水线 (Pipelined requests)
- 目的：批量处理请求，提高 IO 效率，提升服务器吞吐量。请求流水线技术允许客户端一次发送多个请求，而无需等待前一个请求的响应，从而减少了网络延迟，提高了服务器的吞吐能力。
- 流水线原理：客户端可以一次性发送多个请求，服务器按照接收顺序处理这些请求，并按照相同的顺序返回多个响应。客户端收到响应时，需要根据发送请求的顺序来解析响应。
- 解决方法：将输入视为字节流，在 `handle_read()` 函数中循环调用 `try_one_request()`，尽可能地解析和处理缓冲区中的所有完整请求。只要接收缓冲区中有完整的请求消息，就应该尽可能地处理，而不是处理完一个请求就退出 `handle_read()`。
- 修改后的 `handle_read()`：

    ```cpp
    static void handle_read(Conn *conn) {
    // ...
    // try_one_request(conn);               // WRONG
    while (try_one_request(conn)) {}        // CORRECT
    // ...
    }
    ```

#### 2.3. 2 乐观非阻塞写 (Optimistic non-blocking writes)
- 优化思路：在请求-响应协议中，客户端通常在接收到服务器的响应后才会发送下一个请求。因此，服务器可以做出一个乐观的假设：当服务器接收到客户端的请求时，socket 通常是可写的。基于这个假设，服务器可以在 `handle_read()` 函数中尝试直接调用 `handle_write()` 进行写入操作，而无需等待下一次的 `poll()` 系统调用返回可写事件。这样可以减少一次 `poll()` 系统调用，从而降低延迟，提高性能。

- 修改后的 `handle_read()`：
    ```cpp
    static void handle_read(Conn *conn) {
    // ...
    if (conn->outgoing.size() > 0) {    // has a response
        conn->want_read = false;
        conn->want_write = true;
        // The socket is likely ready to write in a request-response protocol,
        // try to write it without waiting for the next iteration.
        return handle_write(conn);      // optimization
    }
    ```
- 同时 `handle_write()`需要增加检查`EAGAIN`确认套接字是否就绪的逻辑

## 3. 更好的缓冲区改进
我根据教程指引改进了使用循环队列代替了原有的`vector<uint8_t>`的缓冲区，具体实现代码参考[Buffer.cpp](./server/buffer.cpp)
```cpp
#ifndef BUFFER_HPP
#define BUFFER_HPP
#include <cstdint>
#include <cstring>
#include <stddef.h>
#include <sys/types.h>

class Buffer {
    public:
        Buffer(size_t capacity=1024);
        ~Buffer();
        void append(const uint8_t *data, size_t len);
        void consume(size_t len);
        void append_u8(uint8_t data);
        void append_u32(uint32_t data);
        void append_i64(int64_t data);
        void append_dbl(double data);
        void insert(const uint8_t *data, size_t len, size_t pos);
        size_t size(void) const;
        bool empty(void) const;
        void peek(uint8_t* dst, size_t pos, size_t len) const;
        uint32_t peek_u32(size_t pos) const;
        void get_continuous_data(size_t pos, uint8_t **data, size_t *size) const;
        void copy_data(uint8_t *dst, size_t len) const;
        void resize(size_t new_capacity);
        uint8_t& operator[](size_t pos);
        const uint8_t& operator[](size_t pos) const;
        
    private:
        size_t head;
        size_t tail;
        size_t capacity;
        size_t _size;
        uint8_t* data;
        
};

#endif
```

## 缓冲区改进逻辑

- 采用循环队列来避免不必要的数据搬移，提升内存使用效率。
- 在 append() 中：  
  - 判断写入数据是否会超出当前容量，如不足则按需求扩容（小于1MB时成倍扩容，大于1MB时每次增加1MB）。
  - 当尾部空间不足时，将数据拆分为两部分：一部分写入尾部，另一部分写入缓冲区起点，然后更新 tail。
- 在 consume() 中：  
  - 直接移动 head 指针并通过取模实现环绕，避免移动数据。
- 在 insert() 中：  
  - 在指定位置（head + pos）处直接覆盖原数据，支持数据环绕写入，
  - 如插入数据超出原有范围，则更新 _size 与 tail，并按需要进行扩容。


PS: 改完之后发现还是正常数组好，可以连续复制。。。使用循环队列需要分段复制缓冲区的内容。