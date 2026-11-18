/**
 * EasyTier WebSocket 中继房间（Durable Object）
 *
 * 使用 WebSocket Hibernation API：连接建立后通过 alarm 驱动心跳，
 * 而不是 setInterval。这样 DO 在空闲时可以被休眠，避免 7×24 计费。
 *
 * @file relay_room.js
 */

import { parseHeader, createHeader } from './core/packet.js';
import { PacketType, HEADER_SIZE, MY_PEER_ID, CloseCode } from './core/constants.js';
import { loadProtos } from './core/protos.js';
import { loadConfig, matchesWsPath } from './core/config.js';
import { PeerManager } from './core/peer_manager.js';
import { GlobalState } from './core/global_state.js';
import { handleHandshake, handlePing, handleForwarding } from './core/basic_handlers.js';
import { handleRpcReq, handleRpcResp } from './core/rpc_handler.js';
import { randomU64String, wrapPacket, sendWithStats } from './core/crypto.js';

const WS_OPEN = 1;
// 加密报文的 AEAD 尾巴：12 字节 nonce + 16 字节 tag，长度校验时需扣除
const AEAD_TAIL_BYTES = 28;

function toUint8Array(message) {
  if (message instanceof ArrayBuffer) return new Uint8Array(message);
  if (ArrayBuffer.isView(message)) {
    return new Uint8Array(message.buffer, message.byteOffset, message.byteLength);
  }
  return null;
}

export class RelayRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.config = loadConfig(env);
    this.types = loadProtos();

    this.globalState = new GlobalState(this.config);
    this.pm = new PeerManager(this.config, this.globalState);
    this.pm.setTypes(this.types);

    this.ctx = {
      config: this.config,
      types: this.types,
      pm: this.pm,
      globalState: this.globalState,
    };

    // 休眠恢复后 socket 上挂载的属性会丢失，统一在 _ensureSocket 中按附件重建
    this._alarmAt = 0;
    this.startedAt = Date.now();
    const sockets = this.state.getWebSockets();
    for (const ws of sockets) {
      this._ensureSocket(ws);
    }
    if (sockets.length > 0) this._scheduleAlarm();
  }

  async fetch(request) {
    const url = new URL(request.url);
    // 内部统计路径：仅由入口 Worker 在令牌校验通过后转发进来
    if (url.pathname === '/__stats') {
      return this._statsResponse(url);
    }
    if (!matchesWsPath(this.config, url.pathname)) {
      return new Response('Not found', { status: 404 });
    }
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected websocket', { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.state.acceptWebSocket(server);
    this._ensureSocket(server);
    this._scheduleAlarm();

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    try {
      this._ensureSocket(ws);

      const buffer = toUint8Array(message);
      if (!buffer) {
        if (this.config.debug) console.warn('[ws] unsupported message type', typeof message);
        return;
      }

      const maxPayload = this.config.maxMessageBytes;
      if (buffer.length < HEADER_SIZE || buffer.length > maxPayload + HEADER_SIZE + AEAD_TAIL_BYTES) {
        if (this.config.debug) console.warn(`[ws] dropped packet with invalid size ${buffer.length}`);
        return;
      }

      const header = parseHeader(buffer);
      if (!header) return;
      if (header.len > maxPayload) return;

      // 声明的 payload 长度必须与实际数据严格对应：
      // 明文时 actual == header.len；加密报文（客户端开启 secure_mode）会多 28 字节
      // 的 AEAD 尾（12 字节 nonce + 16 字节 tag），此时 actual == header.len + 28。
      // 其余情况一律视为截断或夹带报文丢弃。
      const actualPayload = buffer.length - HEADER_SIZE;
      const tailBytes = actualPayload - header.len;
      if (tailBytes !== 0 && tailBytes !== AEAD_TAIL_BYTES) {
        if (this.config.debug) {
          console.warn(`[ws] dropped length mismatch from=${header.fromPeerId} declared=${header.len} actual=${actualPayload}`);
        }
        return;
      }

      ws.lastSeen = Date.now();
      ws.stats.msgsIn += 1;
      ws.stats.bytesIn += buffer.length;
      const payload = buffer.subarray(HEADER_SIZE);

      // 单连接吞吐上限：超出预算的报文直接丢弃（不断连接），避免一条连接打满 DO
      if (!this._allowBytes(ws, buffer.length)) return;

      // 未完成握手的连接只允许发握手包，防止匿名连接直接向其它节点注入流量
      if (!ws.peerId && header.packetType !== PacketType.HandShake) {
        return;
      }

      // EasyTier 客户端回应 Ping 时会把请求头里的 fromPeerId 原样带回（数值等于服务端 ID，
      // toPeerId 又指回自己），因此 Pong 必须在“禁止冒充服务端”检查之前处理：
      // 它只是连接级心跳应答、不参与转发，若当作冒充丢弃，服务端永远收不到 pong，
      // 25 秒后会被误判超时并主动断开，表现为客户端反复掉线重连。
      if (header.packetType === PacketType.Pong) {
        ws.lastPongReceived = Date.now();
        // 用本次 Ping 的发送时间估算中继↔客户端 RTT，异常值（非正数或超过超时）丢弃
        if (ws.lastPingSent) {
          const rtt = ws.lastPongReceived - ws.lastPingSent;
          if (rtt > 0 && rtt <= this.config.connectionTimeoutMs) {
            ws.rttMs = rtt;
          }
        }
        this._updateAttachment(ws);
        return;
      }

      // 禁止冒充服务端身份：客户端之间会互相转发其它节点的报文（fromPeerId 可以不是自己），
      // 所以不能要求 fromPeerId 等于本连接的身份，但任何情况下都不允许使用服务端 MY_PEER_ID，
      // 否则可以伪造控制面报文（路由同步 / RPC 响应）影响其它节点。
      if (Number(header.fromPeerId) === MY_PEER_ID) {
        if (this.config.debug) {
          console.warn(`[ws] dropped packet impersonating server from session=${ws.peerId} type=${header.packetType} to=${header.toPeerId} len=${header.len}`);
        }
        return;
      }

      if (this.config.debug) {
        console.log(`[ws] from=${header.fromPeerId} to=${header.toPeerId} type=${header.packetType} len=${header.len}`);
      }

      const toServer = !header.toPeerId || header.toPeerId === MY_PEER_ID;

      switch (header.packetType) {
        case PacketType.HandShake:
          // 已握手的连接不允许重新握手（否则可借切换分组绕过连接数上限），
          // 但只做忽略、不主动断开：部分客户端会周期性重发握手包，断开会导致反复重连
          if (ws.peerId) {
            if (this.config.debug) {
              console.warn(`[ws] ignored duplicate handshake from ${ws.peerId}`);
            }
            break;
          }
          if (handleHandshake(ws, header, payload, this.ctx)) {
            this._updateAttachment(ws);
          }
          break;

        case PacketType.Ping:
          handlePing(ws, header, payload, this.ctx);
          break;

        case PacketType.RpcReq:
          if (toServer) handleRpcReq(ws, header, payload, this.ctx);
          else {
            // 转发 RPC 的数量很小，始终记录服务名，便于确认客户端的打洞信令
            this._logForwardedRpc(header, payload);
            handleForwarding(ws, header, buffer, this.ctx);
          }
          break;

        case PacketType.RpcResp:
          if (toServer) handleRpcResp(ws, header, payload, this.ctx);
          else handleForwarding(ws, header, buffer, this.ctx);
          break;

        case PacketType.Data:
        default:
          handleForwarding(ws, header, buffer, this.ctx);
          break;
      }
    } catch (e) {
      // 单条报文失败不关闭连接，交给心跳超时兜底
      console.error('relay_room message handling error:', e);
    }
  }

  async webSocketClose(ws) {
    this._ensureSocket(ws);
    if (ws.cleanedUp) return;
    ws.cleanedUp = true;

    const groupKey = ws.groupKey;

    if (ws.peerId) {
      const removed = this.pm.removePeer(ws);
      if (removed) {
        try {
          this.pm.broadcastRouteUpdate(this.types, groupKey, null, { forceFull: true });
        } catch (e) {
          if (this.config.debug) console.warn('[ws] broadcast after close failed:', e.message);
        }
      }
    }

    if (this.state.getWebSockets().length === 0) {
      try {
        await this.state.storage.deleteAlarm();
      } catch (_) {
        // 没有待处理 alarm 时会抛错，忽略即可
      }
    }
  }

  async webSocketError(ws) {
    await this.webSocketClose(ws);
  }

  /**
   * 心跳：用 alarm 代替 setInterval，避免 DO 因为定时器永远不休眠。
   */
  async alarm() {
    this._alarmAt = 0;
    const sockets = this.state.getWebSockets();
    const now = Date.now();

    for (const ws of sockets) {
      this._ensureSocket(ws);

      if (ws.readyState !== WS_OPEN) {
        await this.webSocketClose(ws);
        continue;
      }

      // 握手超时：peerId 未确定的连接不能长期挂在 DO 上（匿名连接会占用配额）
      if (!ws.peerId && now - (ws.firstSeenAt || now) > this.config.handshakeTimeoutMs) {
        if (this.config.debug) console.warn(`[handshake] timeout session=${ws.serverSessionId}`);
        await this.webSocketClose(ws);
        try {
          ws.close(CloseCode.HandshakeTimeout, 'handshake timeout');
        } catch (_) {
          // 已经关闭
        }
        continue;
      }

      // 未完成握手的连接不发心跳，直接交给上面的握手超时处理
      if (ws.peerId && now - (ws.lastPingSent || 0) >= this.config.heartbeatIntervalMs) {
        this._sendPing(ws);
      }

      if (now - (ws.lastPongReceived || now) > this.config.connectionTimeoutMs) {
        if (this.config.debug) console.log(`[heartbeat] timeout peer=${ws.peerId}`);
        await this.webSocketClose(ws);
        try {
          ws.close();
        } catch (_) {
          // 已经关闭
        }
      }

      // 顺便把统计计数写回附件（最多滞后一个心跳周期），避免休眠唤醒后丢失
      this._updateAttachment(ws);
    }

    // 路由同步补偿：3 秒未收到 ack 或有遗留脏标记时重发。
    // 放在 alarm 里，这样休眠唤醒后也能自动补发，不依赖客户端主动请求。
    try {
      this.pm.maintainRouteSync(now);
    } catch (e) {
      console.error('[alarm] maintainRouteSync failed:', e);
    }

    if (this.state.getWebSockets().length > 0) {
      this._scheduleAlarm();
    } else {
      try {
        await this.state.storage.deleteAlarm();
      } catch (_) {
        // 忽略
      }
    }
  }

  _scheduleAlarm() {
    const interval = Math.max(1000, Math.min(this.config.heartbeatIntervalMs, 5000));
    const now = Date.now();
    // 已有更早的排程时不重复覆盖，避免高频建连把心跳一直往后推
    if (this._alarmAt > now) return;

    this._alarmAt = now + interval;
    try {
      const pending = this.state.storage.setAlarm(this._alarmAt);
      if (pending && typeof pending.catch === 'function') pending.catch(() => {});
    } catch (e) {
      this._alarmAt = 0;
      if (this.config.debug) console.warn('[alarm] schedule failed:', e.message);
    }
  }

  _ensureSocket(ws) {
    if (ws.__etReady) {
      if (ws.peerId) this.pm.addPeer(ws.peerId, ws);
      return ws;
    }

    let meta = null;
    if (typeof ws.deserializeAttachment === 'function') {
      meta = ws.deserializeAttachment() || null;
    }
    meta = meta || {};

    ws.__etReady = true;
    ws.cleanedUp = false;
    ws.peerId = meta.peerId || null;
    ws.groupKey = meta.groupKey || null;
    ws.domainName = meta.domainName || null;
    ws.serverSessionId = meta.serverSessionId || randomU64String();
    ws.weAreInitiator = false;
    ws.lastSeen = Date.now();
    // 握手计时起点：只有完成握手（peerId 确定）后才会参与超时检查
    ws.firstSeenAt = meta.firstSeenAt || Date.now();
    ws.lastPingSent = 0;
    // 没有历史 pong 记录时以当前时间起算，避免旧实现里 0 值导致的“永久免死”
    ws.lastPongReceived = meta.lastPongAt || Date.now();
    ws.relayFlags = this.config.latencyFirst ? 0x02 : 0;
    // 最近一次 Ping/Pong 采样得到的 RTT（毫秒），休眠恢复时沿用上次的值
    ws.rttMs = meta.rttMs || 0;
    // 令牌桶：桶容量 = 2 秒的预算，允许短时突发，但持续超速会被丢弃
    ws.bucket = 0;
    ws.bucketAt = 0;

    // 累计流量统计：休眠恢复时从 attachment 还原，避免每次唤醒都从 0 重新计数
    const st = meta.stats || {};
    ws.stats = {
      bytesIn: st.bytesIn || 0,
      bytesOut: st.bytesOut || 0,
      msgsIn: st.msgsIn || 0,
      msgsOut: st.msgsOut || 0,
      connectedAt: st.connectedAt || Date.now(),
    };

    // 出向 1 秒窗口限速，与入向令牌桶对称；窗口计数不需要跨休眠保留
    ws.outLimit = {
      bytesPerSecond: this.config.maxOutBytesPerSecond,
      framesPerSecond: this.config.maxOutFramesPerSecond,
    };
    ws.outWindowAt = 0;
    ws.outBytes = 0;
    ws.outFrames = 0;

    // 路由同步状态随附件持久化：唤醒后若仍处于 inFlight，alarm 会判定超时并强制重发
    const rs = meta.routeSync || null;
    ws.routeSync = {
      inFlight: !!(rs && rs.inFlight),
      dirty: !!(rs && rs.dirty),
      forceFull: !!(rs && rs.forceFull),
      sentAt: (rs && rs.sentAt) || 0,
      lastCompletedAt: (rs && rs.lastCompletedAt) || 0,
    };

    if (ws.peerId) this.pm.addPeer(ws.peerId, ws);
    return ws;
  }

  _updateAttachment(ws) {
    if (typeof ws.serializeAttachment !== 'function') return;
    try {
      ws.serializeAttachment({
        peerId: ws.peerId || null,
        groupKey: ws.groupKey || null,
        domainName: ws.domainName || null,
        serverSessionId: ws.serverSessionId || null,
        lastPongAt: ws.lastPongReceived || Date.now(),
        firstSeenAt: ws.firstSeenAt || Date.now(),
        rttMs: ws.rttMs || 0,
        // 路由同步状态：随附件持久化，休眠唤醒后据此补发/重试
        routeSync: ws.routeSync
          ? {
            inFlight: ws.routeSync.inFlight,
            dirty: ws.routeSync.dirty,
            forceFull: ws.routeSync.forceFull,
            sentAt: ws.routeSync.sentAt,
            lastCompletedAt: ws.routeSync.lastCompletedAt,
          }
          : undefined,
        // 计数随附件一起持久化，休眠唤醒后不会归零
        stats: ws.stats
          ? {
            bytesIn: ws.stats.bytesIn,
            bytesOut: ws.stats.bytesOut,
            msgsIn: ws.stats.msgsIn,
            msgsOut: ws.stats.msgsOut,
            connectedAt: ws.stats.connectedAt,
          }
          : undefined,
      });
    } catch (e) {
      if (this.config.debug) console.warn('[ws] serializeAttachment failed:', e.message);
    }
  }

  /**
   * 令牌桶限速：返回 true 表示本次报文放行。
   * 只丢包不断连接，避免瞬时突发导致客户端整体掉线。
   */
  _allowBytes(ws, bytes) {
    const rate = this.config.maxBytesPerSecond;
    if (!rate) return true;

    const now = Date.now();
    const cap = rate * 2;
    if (!ws.bucketAt) {
      ws.bucket = cap;
      ws.bucketAt = now;
    } else if (now > ws.bucketAt) {
      ws.bucket = Math.min(cap, ws.bucket + ((now - ws.bucketAt) * rate) / 1000);
      ws.bucketAt = now;
    }

    if (ws.bucket < bytes) return false;
    ws.bucket -= bytes;
    return true;
  }

  /**
   * 记录被转发的 RPC 服务名，用于确认客户端之间的打洞信令（如 GetIpList）是否在流动。
   */
  _logForwardedRpc(header, payload) {
    try {
      const rpc = this.types.RpcPacket.decode(payload);
      const d = rpc.descriptor || {};
      console.log(`[rpc-fwd] ${d.serviceName}#${d.methodIndex} ${header.fromPeerId}->${header.toPeerId}`);
    } catch (_) {
      // 解码失败不影响转发
    }
  }

  /**
   * 汇总本房间的负载统计，供入口 Worker 的 /stats 接口返回（令牌在入口校验）。
   * 只统计已完成握手的连接：未握手的连接没有 peerId，无法对应到具体节点。
   * 计数为当前 Durable Object 实例内的累计值，休眠恢复时从附件还原。
   */
  _statsResponse(url) {
    const now = Date.now();
    const room = url.searchParams.get('room') || 'default';
    const peers = [];
    const groups = new Map();
    const totals = { peerCount: 0, bytesIn: 0, bytesOut: 0, msgsIn: 0, msgsOut: 0, latencyMs: 0 };
    let latencySum = 0;
    let latencySamples = 0;

    for (const ws of this.state.getWebSockets()) {
      this._ensureSocket(ws);
      if (!ws.peerId) continue;

      const st = ws.stats;
      const rtt = ws.rttMs || 0;
      const groupKey = ws.groupKey ? String(ws.groupKey) : '';
      const sep = groupKey.indexOf(':');
      const networkName = ws.domainName || (sep > 0 ? groupKey.slice(0, sep) : '');
      const digest = sep >= 0 ? groupKey.slice(sep + 1) : '';
      // 只暴露密钥摘要前缀，不把完整摘要交给调用方
      const group = `${networkName || 'anonymous'}#${digest.slice(0, 8) || 'unknown'}`;

      peers.push({
        peerId: String(ws.peerId),
        group,
        networkName,
        connectedAt: st.connectedAt,
        lastSeen: ws.lastSeen || 0,
        latencyMs: rtt,
        bytesIn: st.bytesIn,
        bytesOut: st.bytesOut,
        msgsIn: st.msgsIn,
        msgsOut: st.msgsOut,
      });

      let g = groups.get(group);
      if (!g) {
        g = { group, networkName, peerCount: 0, latencySum: 0, latencySamples: 0, bytesIn: 0, bytesOut: 0, msgsIn: 0, msgsOut: 0 };
        groups.set(group, g);
      }
      g.peerCount += 1;
      if (rtt > 0) {
        g.latencySum += rtt;
        g.latencySamples += 1;
        latencySum += rtt;
        latencySamples += 1;
      }
      g.bytesIn += st.bytesIn;
      g.bytesOut += st.bytesOut;
      g.msgsIn += st.msgsIn;
      g.msgsOut += st.msgsOut;

      totals.peerCount += 1;
      totals.bytesIn += st.bytesIn;
      totals.bytesOut += st.bytesOut;
      totals.msgsIn += st.msgsIn;
      totals.msgsOut += st.msgsOut;
    }

    // 平均 RTT 只统计有 Ping/Pong 采样的连接；没有连接时保持 0（无法采样）
    totals.latencyMs = latencySamples > 0 ? Math.round(latencySum / latencySamples) : 0;

    // 流量大的排在前面，方便直接看出谁在占带宽
    peers.sort((a, b) => (b.bytesIn + b.bytesOut) - (a.bytesIn + a.bytesOut));

    const groupList = Array.from(groups.values()).map((g) => ({
      group: g.group,
      networkName: g.networkName,
      peerCount: g.peerCount,
      latencyMs: g.latencySamples > 0 ? Math.round(g.latencySum / g.latencySamples) : 0,
      bytesIn: g.bytesIn,
      bytesOut: g.bytesOut,
      msgsIn: g.msgsIn,
      msgsOut: g.msgsOut,
    }));

    const body = {
      room,
      startedAt: this.startedAt,
      uptimeMs: now - this.startedAt,
      generatedAt: now,
      totals,
      groups: groupList,
      peers,
    };
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      },
    });
  }

  _sendPing(ws) {
    ws.lastPingSent = Date.now();
    try {
      if (ws.readyState !== WS_OPEN) return;
      const payload = new Uint8Array([112, 105, 110, 103]); // "ping"
      sendWithStats(ws, wrapPacket(createHeader, MY_PEER_ID, ws.peerId || 0, PacketType.Ping, payload, ws));
      if (this.config.debug) console.log(`[heartbeat] ping peer=${ws.peerId}`);
    } catch (e) {
      if (this.config.debug) console.warn(`[heartbeat] ping failed peer=${ws.peerId}: ${e.message}`);
    }
  }
}
