# SDK架构设计

Author: qians

Date: 2023/6/6

Version: v0.0.1

## 需求

- tcp读消息
  - 粘包拆包
  - 加解密
  - 压缩解压缩
  - ACK消息
- tcp发消息
- 维持心跳
- 断线重连

## struct

```go
struct Connection {
    url string
    isCompress bool
    authMessage string
    heartbeatInterval int
    tcp *net.tcp // ?好像是这个, 反正就是连接
    isHandled bool
    onClose func()
}
```

## 接口

网络模块对外暴露4个方法: login, logout, handle, send

### login

> Login构造方法, 返回Connection结构体. 构造数据结构, 建立连接, 开启心跳但不开启读.

- 请求参数

| 参数名            | 参数类型 | 说明          | 是否必填 |
| ----------------- | -------- | ------------- | -------- |
| authMessage       | String   | 鉴权消息      | 是       |
| url               | String   | tcp服务器地址 | 是       |
| heartbeatInterval | int      | 心跳间隔      | 是       |
| isCompress        | bool     | 是否开启压缩  | 是       |
| onClose           | func()   | 关闭回调      | 否       |
| ...               | ...      | ...           | ...      |

- 返回值

Login数据结构, 不需要对外暴露内部变量

### logout

> Login析构方法, 释放资源, 关闭相关goroutine

无请求参数, 无返回值

```go
func (c *Connection) logout() {
    // close resources
    tcp.Close()
    c.onClose()
}
```

### handle

> 开启读, 处理TCP拆包粘包, 加解密, 解压缩. 并将处理完成的数据协议中的业务消息体回调给上游

- 请求参数

| 参数名 | 参数类型          | 说明       | 是否必填 |
| ------ | ----------------- | ---------- | -------- |
| onRead | func(data []byte) | 读消息回调 | 是       |

```go
func (c *Connection) handle(onRead func(data []byte)) {
    if c.isHandled {
        // you cannot handle twice
        return
    }

    for {
        // process data from tcp
        // ...
        tcp.read
        // call onRead func
        go func() {
            onRead(contentBytes)
            // do ack
            c.ack()
        }()
    }
}
```
