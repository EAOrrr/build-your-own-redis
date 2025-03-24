# 套接字编程

套接字 (Socket) 是用来标识连接或其他网络资源的句柄。网络编程中提供的接口通常被称为套接字 API。

## 1. 套接字API概述

### 1.1 创建监听套接字
创建一个监听套接字通常需要如下三个操作：
1. 调用 `socket()` 获取套接字句柄；
2. 用 `bind()` 指定监听的 IP 与端口；
3. 调用 `listen()` 将套接字置于监听状态。

伪代码：
```c
fd = socket();
bind(fd, address);
listen(fd);
while (true) {
    conn_fd = accept(fd);
    do_something_with(conn_fd);
    close(conn_fd);
}
```
### 1.2 发起连接
对于客户端建立连接，需要调用以下两个操作：
1. 调用 `socket()` 获取套接字句柄；
2. 使用 `connect()` 建立连接。

伪代码：
```c
fd = socket();
connect(fd, address);
do_something_with(fd);
close(fd);
```

### 1.3 进行数据传输
对套接字进行数据传输时，可使用以下操作：
- 使用 `read()` 或 `recv()` 读取数据；
- 使用 `write(`) 或 `send()` 发送数据；
- 使用 `close()` 关闭连接。

## 2. 实战示例
### 2.1 创建 TCP 服务器

#### 步骤 1：创建套接字  
`socket()` 函数需要三个整数参数。示例：
```c
int fd = socket(AF_INET, SOCK_STREAM, 0);
```
这三个参数决定了套接字的类型，其中：
- `AF_INET` 表示 IPv4；若使用 IPv6或需要支持双栈，使用 `AF_INET6；`
- `SOCK_STREAM` 用于 TCP 连接；UDP 使用 `SOCK_DGRAM；`
- 第三个参数通常为 0。

常用参数组合：
| 协议      | 参数                               |
| --------- | ---------------------------------- |
| IPv4 TCP  | `socket(AF_INET, SOCK_STREAM, 0)`    |
| IPv6 TCP  | `socket(AF_INET6, SOCK_STREAM, 0)`   |
| IPv4 UDP  | `socket(AF_INET, SOCK_DGRAM, 0)`     |
| IPv6 UDP  | `socket(AF_INET6, SOCK_DGRAM, 0)`    |

#### 步骤 2：设置套接字选项  
使用 `setsockopt()` 设置 `SO_REUSEADDR` 选项，使服务器在重启后仍能绑定同一 IP:端口：
```c
int val = 1;
setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
```

#### 步骤 3：绑定地址  
将套接字绑定到通配符地址（例如 0.0.0.0:1234）。示例：
```c
struct sockaddr_in addr = {};
addr.sin_family = AF_INET;
addr.sin_port = htons(1234);        // 端口号
addr.sin_addr.s_addr = htonl(0);      // 通配符地址 0.0.0.0
int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
if (rv) { die("bind()"); }
```
说明：`struct sockaddr_in` 用于存放 IPv4 地址和端口，端口号使用 `htons()` 转换为网络字节序，IP 地址通过 `htonl()` 转换。

#### 步骤 4：监听  
调用 `listen()` 启动监听状态。操作系统接管 TCP 握手并将已建立的连接放入等待队列。
```c
rv = listen(fd, SOMAXCONN);
if (rv) { die("listen()"); }
```
`SOMAXCONN` 为系统允许的最大连接排队数（Linux 上通常为 4096）。

#### 步骤 5：接受连接  
服务器循环调用 `accept()` 获取客户端连接，并对每个连接进行处理：
```c
while (true) {
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0) {
        continue;   // 出现错误则跳过
    }
    do_something(connfd);
    close(connfd);
}
```

### 2.2 创建 TCP 客户端

客户端示例通过以下步骤建立连接、发送消息并接收响应：
```c
int fd = socket(AF_INET, SOCK_STREAM, 0);
if (fd < 0) {
    die("socket()");
}

struct sockaddr_in addr = {};
addr.sin_family = AF_INET;
addr.sin_port = htons(1234); // 与服务器保持一致
addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // 本地回环地址 127.0.0.1
int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
if (rv) {
    die("connect");
}

char msg[] = "hello";
write(fd, msg, strlen(msg));

char rbuf[64] = {};
ssize_t n = read(fd, rbuf, sizeof(rbuf) - 1);
if (n < 0) {
    die("read");
}
printf("server says: %s\n", rbuf);
close(fd);
```
其中，`INADDR_LOOPBACK` 定义为 0x7f000001，代表地址 127.0.0.1。

