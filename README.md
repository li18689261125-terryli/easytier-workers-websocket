# Easytier-Workers-Websocket

基于 Cloudflare Workers + Durable Objects 的 EasyTier WebSocket 服务端实现。使用 JavaScript 复刻 EasyTier 协议（v4）的握手、路由同步与数据转发，官方 Rust 客户端可以直接连接，无需改动任何客户端代码。

> **本项目仅供学习交流使用，请勿用于商业用途或非法用途。**

## 项目简介

EasyTier 是一个去中心化的 P2P 组网工具，官方实现基于 Rust。本项目把 WebSocket 这一层的中继服务端搬到 Cloudflare 边缘运行时上：

- 每个 `?room=` 对应一个 Durable Object，同一实例内再按网络组（`network_name` + 网络密钥摘要）隔离，跨组流量不互通
- 服务端只做控制面同步与必要的数据转发，并主动交换 STUN / 公网可达性信息帮助客户端打洞
- 协议编解码使用 protobufjs 静态生成的代码，与官方 `.proto` 保持一致

## 主要特性

| 特性 | 说明 |
|---|---|
| 直连即用 | `WS_PATH` 留空时任意路径都接受，客户端填 `wss://你的域名` 即可，无需 `/ws` |
| 服务器标识 | 节点列表中显示为 `Easytier-Workers-Websocket` |
| 心跳与清理 | 主动 Ping/Pong，超时立即清理，断线节点从所有客户端列表消失 |
| 低成本 | 心跳由 Durable Object `alarm` 驱动，没有连接时自动休眠，不产生常驻计费 |
| 私有网络 | 配置网络名后只接受同名客户端；再配置网络密钥后，只有持有相同密钥的客户端才能加入 |
| 延迟优先 | 特权标志位让客户端优先走本节点收敛路由 |
| 打洞优先 | 先按星型下发保证可通；客户端打洞成功后上报直连关系，服务端并进位图再广播，流量自动切到 P2P，打洞失败则一直走中继 |
| 纯 P2P 模式 | 只转发控制面，不转发 Data 包，强制客户端自行打洞 |
| PeerCenter | 支持 PeerMap / GlobalPeerMap 查询，便于客户端做全局发现 |

## 技术架构

- **运行时**：Cloudflare Workers（`compatibility_date = 2024-11-01`，开启 `nodejs_compat`）
- **状态**：Durable Object（SQLite 存储后端），单一 `RelayRoom` 类
- **传输**：WebSocket 二进制帧，自定义 16 字节包头 + Protobuf 负载
- **序列化**：protobufjs + long，代码位于 `src/worker/core/protos_generated.js`
- **状态管理**：所有对等节点表、路由会话、版本号都是 `RelayRoom` 实例字段，不跨实例共享

## 一键部署

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/Teleseon/Easytier-Workers-Websocket)

点击后按提示授权 GitHub 与 Cloudflare，会自动拉取仓库、创建 Worker 并应用 `RelayRoom` Durable Object 的迁移，无需本地环境。

几点说明：

- 部署完成后到 Cloudflare 面板 → Workers → 你的 Worker → 设置 → 变量和机密，按[环境变量](#环境变量)调整 `WS_PATH`、`EASYTIER_NETWORK_NAME` 等；不填则使用 `wrangler.toml` 里的默认值
- `WS_PATH` 留空表示任意路径都接受，客户端直接填 `wss://<你的域名>` 即可
- Durable Object 需要迁移才能创建，首次部署由 `[[migrations]]` 自动完成；若在面板里手工建 Worker 则需自己补上绑定

## 快速开始

### 前置要求

- Node.js >= 22.12（wrangler 4 的最低要求）
- npm
- Cloudflare 账号

### 本地开发

```bash
npm install
npx wrangler login      # 仅首次需要
npm run dev             # 监听 0.0.0.0:8787，改代码自动热重载
```

### 部署

#### 方式一：GitHub Actions（推送到 main 即自动部署）

仓库自带 `.github/workflows/deploy.yml`，推送到 `main` 或手动触发 `Deploy to Cloudflare Workers` 就会自动部署。

需要在 **GitHub 仓库 → Settings → Secrets and variables → Actions** 里添加两个 secret：

| Secret | 说明 |
|---|---|
| `CLOUDFLARE_API_TOKEN` | Cloudflare 面板 → 我的个人资料 → API 令牌 → 创建令牌，选「编辑 Cloudflare Workers」模板 |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare 面板首页右侧的 Account ID |

#### 方式二：本地部署

```bash
npx wrangler login
npx wrangler deploy
```

## 客户端连接

服务端不限制路径，填一个地址即可：

| 环境 | 填写 |
|---|---|
| 本地开发 | `ws://你的局域网IP:8787` |
| 线上部署 | `wss://你的域名` |

几点说明：

- 本地开发用 `ws://`，不要用 `wss://`（本地没有 TLS）
- 建议用局域网 IP 而不是 `127.0.0.1`：如果 EasyTier 绑定了物理网卡，连回环地址会报 `10049 AddrNotAvailable`
- 端口填 `0` 表示使用协议默认端口（ws = 80，wss = 443）
- 若想固定路径，把 `WS_PATH` 设成 `ws`，此时客户端要填 `wss://域名/ws`（`/ws/` 同样可用）

## 私有网络与网络密钥

默认是公开服务器，任何客户端都能接入。要限制访问，按「网络名 → 网络密钥」两级收紧：

1. 配置网络名：只有同名客户端能加入，其它网络名握手时直接以 `4403` 关闭。
2. 再配置网络密钥：服务端按 EasyTier 相同算法计算摘要，与客户端上报的 `network_secret_digest` 做常量时间比对，密钥不同同样以 `4403` 关闭。

```toml
# wrangler.toml
[vars]
EASYTIER_NETWORK_NAME = "my_net"
```

```bash
# 密钥不要写进 wrangler.toml，用 secret 保存
npx wrangler secret put EASYTIER_NETWORK_SECRET
```

客户端侧使用同样的网络名与密钥：

```bash
easytier-core \
  --network-name my_net \
  --network-secret 你的密钥 \
  -p wss://你的域名
```

几点说明：

- 密钥摘要的计算依赖网络名，因此 `EASYTIER_NETWORK_SECRET` 必须与 `EASYTIER_NETWORK_NAME` 同时配置，否则 `/healthz` 会返回 `503`。
- 未配置密钥时服务端只把客户端上报的摘要当作分组标识：别人可以连上这个服务器，但只有网络名和摘要都一致才会进入你的组；由于摘要由客户端自行上报、可以伪造，这种模式不适合安全隔离。
- 密钥不匹配时客户端日志表现为 `conn closed during wait handshake response`，服务端 debug 日志会打印 `network secret mismatch`。
- 摘要算法与 EasyTier 官方实现一致（Rust `DefaultHasher`，SipHash-1-3），已在 EasyTier 2.6.4 客户端实测通过。

## 客户端侧要求（P2P 相关）

打洞能不能建立，**取决于客户端配置，服务端无法代劳**。常见排查顺序：

1. **网络配置里必须开启 P2P**（不要勾选「禁用 P2P」）。EasyTier 客户端上报的 `disable_p2p` 为 `true` 时，它既不会发起打洞，其它节点看到这个标记也不会去连它，结果是双方一次 `GetIpList` 都不会发，节点列表永远显示 `relay(2)`。
2. 改完客户端配置后要**断开该网络配置再重连**才生效。
3. 打洞成功的前提是能建立直连隧道，因此候选地址的选择由客户端决定。如果两端在同一局域网，但延迟仍有几十毫秒抖动，通常是客户端挑了公网地址（`240e:` 这类 IPv6 或 WAN IPv4），或是底层链路本身是无线接入 —— 用 `ping` 对端局域网 IP 即可区分（网关 0ms 而对端几十毫秒，说明是终端侧的无线链路问题，与本服务无关）。
4. 服务端不参与地址选择，只负责把 `GetIpList` 这类 peer→peer 的 RPC 转发出去。

## 幽灵节点是怎么来的

节点列表里偶尔会出现「同名、但虚拟 IP 不同、没有延迟也没有流量」的条目，通常还挂着 `relay(2)`。这不是服务端漏删，原因如下：

1. **EasyTier 每次进程启动或重连都会生成新的 `peer_id`**，而虚拟 IP 是按 `peer_id` 分配的。所以同一台机器重启一次，就会从 `10.126.126.1` 变成 `10.126.126.3`。
2. 服务端只能按 `peer_id` 登记节点，新旧两个 `peer_id` 在它看来就是两个不同的节点，无法判断它们来自同一台机器（`hostname` 可以重复，不能作为身份）。
3. 旧连接的 WebSocket 往往是**半开**状态：TCP 层还没断，服务端也就收不到关闭事件，只能靠心跳判定。

因此旧条目会在 `EASYTIER_CONNECTION_TIMEOUT`（默认 25 秒）内没有收到 Pong 后被清理，并向所有节点广播路由更新，条目随之消失。

判断与处理：

| 现象 | 结论 |
|---|---|
| 25 秒 ~ 1 分钟内自己消失 | 正常，属于重连产生的短暂残留 |
| 长时间（超过 1 分钟）一直存在 | 旧连接实际仍存活，或该 `peer_id` 仍被某个客户端实例占用 |
| 频繁出现 | 说明链路在反复抖动导致不断重连，优先排查网络（见下方「链路质量」） |

如果希望残留更快消失，可以同时调小两个值（`[vars]` 中）：

```toml
EASYTIER_HEARTBEAT_INTERVAL = "5000"   # 5 秒一次心跳
EASYTIER_CONNECTION_TIMEOUT = "15000"  # 15 秒无 Pong 即清理
```

代价是链路抖动时更容易误判掉线，不建议低于 5 秒 / 15 秒。

> 服务端对「已握手的连接再次握手」采取的是**忽略**而不是断开，避免某些客户端周期性重发握手包时被反复踢下线 —— 这种误踢会表现为「高丢包 + 幽灵节点反复出现」。

### 链路质量

幽灵节点频繁出现往往只是症状，根因是客户端到中继的链路在抖。国内直连 Cloudflare 时尤其明显，实测常见 `ICMP` 就已有 300ms 左右、TCP 握手偶发 1 秒以上，此时 EasyTier 会显示很高的延迟和丢包并不断重连。

先区分是「中继的问题」还是「链路的问题」：

```bash
ping -n 50 你的域名            # 丢包 >2% 或抖动 >100ms 说明是线路问题
curl -o NUL -s -w "%{time_total}\n" https://你的域名/healthz
```

如果 ICMP 本身就慢或丢包，任何服务端代码优化都无法改善，建议改用离你更近的中继节点（例如自建 VPS），而不是继续在 Worker 侧找原因。

## 环境变量

普通开关放在 `wrangler.toml` 的 `[vars]` 中；密钥类变量（`EASYTIER_NETWORK_SECRET`、`EASYTIER_STATS_TOKEN`）建议用 `npx wrangler secret put` 保存，避免提交到仓库。

### 入口与身份

| 变量 | 默认值 | 说明 |
|---|---|---|
| `WS_PATH` | `""` | WebSocket 路径；留空 = 任意路径 |
| `EASYTIER_VERSION` | `Easytier-Workers-Websocket` | 上报给客户端的版本号 |
| `EASYTIER_HOSTNAME` | `Easytier-Workers-Websocket` | 上报给客户端的节点名 |
| `EASYTIER_NETWORK_NAME` | `""` | 私有网络名；留空为公开服务器 |
| `EASYTIER_NETWORK_SECRET` | `""` | 网络密钥；配置后强制校验客户端上报的密钥摘要，只有相同密钥才能加入（需同时配置 `EASYTIER_NETWORK_NAME`，建议用 `wrangler secret put` 保存） |
| `EASYTIER_PUBLIC_SERVER_NETWORK_NAME` | `public_server` | 公开服务器对外展示的网络名 |
| `EASYTIER_NETWORK_LENGTH` | `24` | 虚拟网段前缀长度 |

### 心跳与超时

| 变量 | 默认值 | 说明 |
|---|---|---|
| `EASYTIER_HEARTBEAT_INTERVAL` | `10000` | 心跳间隔（毫秒） |
| `EASYTIER_CONNECTION_TIMEOUT` | `25000` | 连接超时（毫秒），超时即断开并清理；必须大于心跳间隔 |
| `EASYTIER_HANDSHAKE_TIMEOUT` | `10000` | 握手超时（毫秒），超时未完成握手以 `4408` 关闭 |
| `EASYTIER_SESSION_TTL_MS` | `180000` | 路由会话状态保留时长 |

### 行为开关

| 变量 | 默认值 | 说明 |
|---|---|---|
| `EASYTIER_LATENCY_FIRST` | `0` | `1` 开启延迟优先模式（本仓库 `wrangler.toml` 默认设为 `1`） |
| `EASYTIER_DISABLE_RELAY` | `0` | `1` 开启纯 P2P，不转发 Data 包 |
| `EASYTIER_FULL_MESH` | `0` | `1` 强制下发全互联位图（复现旧版行为，仅排查用；会导致打洞后流量仍走中继） |
| `EASYTIER_DEBUG` | `0` | `1` 输出逐包调试日志（会明显拉高延迟，生产勿开） |
| `LOCATION_HINT` | `wnam` | Durable Object 部署区域 |
| `EASYTIER_OSPF_ROUTE_METHOD_INDEX` | `1` | 下发给客户端的路由方法索引 |
| `EASYTIER_HANDSHAKE_MODE` | `foreign` | 握手时的网络名回填策略 |
| `EASYTIER_PEER_CENTER_TTL_MS` | `180000` | PeerCenter 缓存过期时间 |

### 防护上限

| 变量 | 默认值 | 说明 |
|---|---|---|
| `EASYTIER_MAX_MESSAGE_BYTES` | `1048576` | 单帧报文上限（字节） |
| `EASYTIER_MAX_PEERS_PER_GROUP` | `512` | 单个网络组最大连接数 |
| `EASYTIER_MAX_BYTES_PER_SEC` | `16777216` | 单连接入向吞吐上限（字节/秒），超出部分丢弃 |
| `EASYTIER_MAX_OUT_BYTES_PER_SEC` | `33554432` | 单连接出向上限（字节/秒，1 秒滚动窗口），超出报文丢弃、不断连接 |
| `EASYTIER_MAX_OUT_FRAMES_PER_SEC` | `4096` | 单连接出向帧数上限（帧/秒），防广播风暴与慢客户端堆积 |
| `EASYTIER_MAX_DIRECT_PEERS` | `256` | 单次 PeerCenter 上报的直连节点上限 |
| `EASYTIER_MAX_PEER_CENTER_ENTRIES` | `4096` | PeerCenter 全局表条目上限，超限淘汰最旧条目 |

## 健康检查

`GET /healthz` 会先做配置自检：数值型环境变量格式、`EASYTIER_CONNECTION_TIMEOUT > EASYTIER_HEARTBEAT_INTERVAL`、`WS_PATH` 字符集、`LOCATION_HINT` 地域码、`EASYTIER_NETWORK_SECRET` 是否配了网络名、`RELAY_ROOM` 绑定。全部通过返回 `200 ok`；有问题返回 `503` 和 JSON 错误列表，响应均带 `Cache-Control: no-store`。

## 关闭码

| 关闭码 | 含义 |
|---|---|
| `4401` | 握手非法：magic 不符、peerId 非法、密钥摘要长度不对 |
| `4403` | 网络名不匹配（私有模式），或网络密钥摘要校验失败 |
| `4408` | 超过 `EASYTIER_HANDSHAKE_TIMEOUT` 仍未完成握手 |

## 负载统计接口

| 变量 | 默认值 | 说明 |
|---|---|---|
| `EASYTIER_STATS_TOKEN` | `""` | `GET /stats` 的 Bearer 令牌；留空 = 不启用该接口（返回 404） |

配置 `EASYTIER_STATS_TOKEN` 后，Worker 提供 `GET /stats`，返回当前中继的连接数与累计收发流量，便于自己查负载或交给监控程序采集。

```bash
# 通过 secret 配置（推荐，不会写进 wrangler.toml）
npx wrangler secret put EASYTIER_STATS_TOKEN

curl -H "Authorization: Bearer 你的令牌" "https://你的域名/stats?room=default"
```

- 未配置 `EASYTIER_STATS_TOKEN` 时返回 `404`，等同于未启用，也不暴露该接口是否存在。
- 令牌放在 `Authorization: Bearer` 头里，服务端先 SHA-256 再定长比较。
- `?room=` 指定房间，默认 `default`；不认识的值会回落到 `default`。
- 只统计已完成握手的连接（未握手的连接没有 `peerId`）。

响应示例（节选）：

```json
{
  "room": "default",
  "startedAt": 1763260000000,
  "uptimeMs": 3600000,
  "generatedAt": 1763263600000,
  "totals": { "peerCount": 2, "latencyMs": 42, "bytesIn": 1048576, "bytesOut": 2097152, "msgsIn": 4096, "msgsOut": 8192 },
  "groups": [
    { "group": "public_server#a1b2c3d4", "networkName": "public_server", "peerCount": 2, "latencyMs": 42, "bytesIn": 1048576, "bytesOut": 2097152, "msgsIn": 4096, "msgsOut": 8192 }
  ],
  "peers": [
    { "peerId": "123456789", "group": "public_server#a1b2c3d4", "networkName": "public_server", "connectedAt": 1763260000000, "lastSeen": 1763263599000, "latencyMs": 40, "bytesIn": 524288, "bytesOut": 1048576, "msgsIn": 2048, "msgsOut": 4096 }
  ]
}
```

几点说明：

- `latencyMs` 是中继侧 Ping/Pong 心跳采样的 RTT（毫秒），反映**客户端↔中继**的链路延迟：`totals` / `groups` 是平均值，`peers` 是该连接最近一次采样值。没有连接或还没完成一次采样时为 `0`（中继无法凭空测出到某个节点的延迟）。
- `bytesIn` / `bytesOut` 是 WebSocket 报文大小的累计值（含中继转发的数据面），不是 Cloudflare 计费口径；`msgsIn` / `msgsOut` 是报文条数。
- 计数随连接附件持久化（每次 alarm 心跳回写一次，最长滞后约 5 秒）；Durable Object 被重新调度后从最后一次落盘值继续，`startedAt` / `uptimeMs` 会按新实例重新计算。
- `group` 里的密钥摘要只保留前 8 位，不把完整摘要交给调用方。
- `peers` 按收发总量从大到小排序，便于直接看出是谁在占带宽。

## Durable Object 地区

Durable Object 默认就近调度。需要固定时可设置 `LOCATION_HINT`：

| 值 | 地区 | 值 | 地区 |
|---|---|---|---|
| `wnam` | 北美西部 | `eeur` | 东欧 |
| `enam` | 北美东部 | `apac` | 亚太（默认） |
| `sam` | 南美 | `oc` | 大洋洲 |
| `weur` | 西欧 | `afr` | 非洲 |
| `me` | 中东 | | |

详见 [Cloudflare 官方文档](https://developers.cloudflare.com/durable-objects/reference/data-location/)。

## 项目结构

```
.
├── protos/                     # Protocol Buffers 定义（common / error / peer_rpc）
├── src/
│   ├── worker.js               # Worker 入口：健康检查、/stats 统计、路径校验、转发到 DO
│   └── worker/
│       ├── relay_room.js       # Durable Object：连接生命周期、心跳、分发
│       └── core/
│           ├── config.js       # 环境变量解析与路径匹配
│           ├── constants.js    # 协议常量（包头、包类型、服务器 peer id）
│           ├── packet.js       # 16 字节包头编解码
│           ├── protos.js       # protobuf 加载
│           ├── protos_generated.js  # pbjs 静态生成代码
│           ├── basic_handlers.js    # 握手、Ping、数据转发
│           ├── network_secret.js    # 网络密钥摘要（SipHash-1-3）与常量时间比较
│           ├── rpc_handler.js       # RPC 请求/响应（路由同步、PeerCenter）
│           ├── peer_manager.js      # 节点表、连接位图、路由广播
│           ├── global_state.js      # PeerCenter 缓存与摘要
│           ├── compress.js          # 入站压缩体兼容处理
│           └── crypto.js            # 随机 ID、包头封装与发送流量统计
├── package.json
├── wrangler.toml
└── README.md
```

## 实现要点

### 心跳与幽灵节点

服务端每 `EASYTIER_HEARTBEAT_INTERVAL` 发一次 Ping，超过 `EASYTIER_CONNECTION_TIMEOUT` 没有 Pong 就断开并广播路由更新。定时任务由 Durable Object 的 `alarm` 驱动，房间空了会取消下一次 alarm，因此空闲时实例可以正常休眠。

### 路由同步与打洞

节点加入时广播一次全量路由，之后按拓扑变化增量推送。连接位图按拓扑签名缓存，避免每个目标都重建一次；版本号在进程内单调递增，杜绝重启后回退导致的客户端不刷新。

连接位图是 P2P 能否生效的关键。语义是 `n×n` 位、`bit(i,j)=1` 表示两个节点直连，服务端按下面的规则生成：

- 服务器与各节点的直连边（星型底座，保证任何时候数据都能经中继送达）
- 各客户端上报的直连边（`SyncRouteInfoRequest.conn_bitmap`）

客户端打洞成功后会在下一次路由同步里带上自己的 `conn_map`，服务端把它并入全局位图并立即广播，其它节点据此把 `next_hop` 指向对端，数据面从「经中继」切到「直连」。这些边不做超时淘汰，生命周期跟随会话：客户端只在连接关系变化时才上报，若按时效淘汰，直连明明还在也会被误删，路由会退回中继。

位图签名包含位图内容本身，否则「peer 列表不变但连接关系变化」时版本号不递增，客户端会忽略这次更新。

### 协议兼容性

- 出站 RPC 响应固定使用 `algo = 1 (None)`，不使用服务端压缩（部分客户端会按声明算法解压，标错会导致解析失败）
- 入站若收到压缩体，会做一次尽力解压，失败则丢弃该包而不是断开连接
- 服务器 `peer_id` 固定，客户端若冒充该 ID 会被直接拒绝

## 安全边界

先说清楚一点：EasyTier 的端到端加密发生在客户端之间，本服务只是中继，**不参与密钥协商，也不解密数据面**。因此这里做的是「不放大攻击面、不成为注入点」，而不是自己造一套加密。

已实现的防护：

- **来源校验**：未完成握手的连接只能发握手包；任何报文都不允许使用服务端 `peer_id`，否则可以伪造路由同步或 RPC 响应影响其它节点。客户端代转其它节点的报文是允许的（`fromPeerId` 可以不是自己），这是 EasyTier 多跳中继的既有语义。
- **分组隔离**：`groupKey = 网络名 + 网络密钥摘要`，跨组一律不转发；私有模式（`EASYTIER_NETWORK_NAME`）下网络名不匹配直接拒绝。
- **密钥校验**：配置 `EASYTIER_NETWORK_SECRET` 后，服务端用网络名 + 密钥按 EasyTier 相同算法算出摘要，与客户端握手上报的 `network_secret_digest` 做常量时间比对，不一致立刻以 `4403` 关闭，密钥不同的客户端无法进入同名网络。摘要计算依赖网络名，二者必须同时配置。
- **输入校验**：报文总长、头部声明长度、网络名长度（≤64）、密钥摘要长度（必须为空或 32 字节）都会校验，畸形报文直接丢弃而不是中断服务。
- **资源上限**：单连接令牌桶限速、单组连接数上限、PeerCenter 全局表条目上限，超限丢包但不主动断连，避免一条坏连接拖垮整个房间。
- **不允许重复握手**：已握手的连接再次握手会被忽略（不重新登记，也不断开），既防止借机切换分组绕过连接数上限，也避免客户端周期性重发握手包时被误踢下线。
- **实例名收敛**：`?room=` 只接受 `[A-Za-z0-9_-]{1,64}`，非法值回退到 `default`。

仍需注意的限制：

- 服务端**都不配置**时是公开服务器：任何网络名、任何密钥的客户端都能握手成功，并按自己上报的「网络名 + 密钥摘要」自动分组，不同组之间互相不可见。此时只有网络名和密钥都一致才会进入同一个虚拟网络；如果自己的网络设了密钥，别人仅知道网络名进不了你的组，但摘要由客户端自行上报、可以伪造，因此敏感场景仍应配置 `EASYTIER_NETWORK_SECRET` 让服务端校验。
- `from_peer_id` 由客户端自己填写，协议层面没有端到端身份认证；TLS 只覆盖「客户端 ↔ Cloudflare」这一段。客户端之间的信任依赖 EasyTier 自身的设计。
- 线上务必使用 `wss://`，不要用明文的 `ws://`。
- 如果要防止他人滥用你的 Worker 配额，建议在 Cloudflare 面板开启 Rate limiting 规则或加来源限制。

## 已知限制

- 只实现了服务端所需的协议子集，不包含官方 Rust 核心的完整路由与加密能力
- 中继带宽受 Cloudflare 配额限制，纯 P2P 模式下建议开启 `EASYTIER_DISABLE_RELAY=1`
- 打洞与地址选择完全由客户端决定，服务端只转发信令。客户端未开启 P2P、或底层链路本身抖动（典型是全无线接入）时，表现出来的高延迟与本服务无关，先用 `ping` 对端局域网 IP 排除
- 项目仍在早期阶段，欢迎反馈问题


欢迎提交 Issue 和 Pull Request。

## 许可证

[MIT License](./LICENSE)

## 免责声明

本项目仅供学习交流使用，请勿用于任何商业用途或非法用途。使用本项目代码造成的任何后果，原作者概不负责。
