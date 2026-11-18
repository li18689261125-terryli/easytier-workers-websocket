/**
 * EasyTier 对等节点管理器
 *
 * 负责：连接注册、路由同步（SyncRouteInfoRequest）与连接位图生成。
 *
 * 与旧实现的主要差异：
 * - 实例化（由 RelayRoom 持有），不再使用模块级单例，避免跨 Durable Object 串台；
 * - 所有配置来自 config.js，不再读取 process.env；
 * - 删除了从未启用的虚拟 IP 分配逻辑与从未被使用的连接版本号表；
 * - 位图改为按拓扑签名缓存 + fill(0xff)，广播时不再对每个目标重建 O(N²) 位图，
 *   子设备查找改为预建 Set，整体从 O(N³) 降到 O(N²)。
 *
 * @file peer_manager.js
 */

import { MY_PEER_ID, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String, sendWithStats } from './crypto.js';

const WS_OPEN = 1;
// 路由同步请求在途超时：超时未收到 SyncRouteInfoResponse 就强制全量重发
const ROUTE_SYNC_TIMEOUT_MS = 3000;

function randomUint32() {
  return Math.floor(Math.random() * 4294967296);
}

function makeInstId() {
  return {
    part1: randomUint32(),
    part2: randomUint32(),
    part3: randomUint32(),
    part4: randomUint32(),
  };
}

export class PeerManager {
  constructor(config, globalState) {
    this.config = config;
    this.globalState = globalState;

    this.peersByGroup = new Map(); // groupKey -> Map(peerId -> ws)
    this.peerInfosByGroup = new Map(); // groupKey -> Map(peerId -> peerInfo)
    this.routeSessions = new Map(); // groupKey -> Map(peerId -> session)
    this.bitmapCache = new Map(); // groupKey -> { sig, bitmap }
    // 各客户端上报的连接关系（打洞成功后由客户端自己声明），groupKey -> Map(peerId -> {edges, ts})
    this.reportedEdges = new Map();

    this.types = null;
    this.myInfo = null;
    this.pureP2PMode = !!config.disableRelay;
    // 默认全互联位图：让客户端尝试 P2P 打洞，失败才回落中继
    this.fullMesh = config.fullMesh !== false;
    this.globalNetworkVersion = 0;

    this.sessionTtlMs = config.sessionTtlMs;
    this.lastSessionCleanup = 0;
  }

  setTypes(types) {
    this.types = types;
  }

  ensureMyInfo() {
    if (this.myInfo) return this.myInfo;
    const c = this.config;
    this.myInfo = {
      peerId: MY_PEER_ID,
      instId: makeInstId(),
      cost: 1,
      version: 1,
      featureFlag: {
        isPublicServer: true,
        avoidRelayData: this.pureP2PMode,
        kcpInput: false,
        noRelayKcp: false,
      },
      networkLength: Number(c.networkLength || 24),
      easytierVersion: c.easytierVersion,
      lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
      hostname: c.hostname,
      // 声明为 OpenInternet，鼓励客户端之间直接打洞
      udpStunInfo: 1,
      peerRouteId: randomU64String(),
      groups: [],
    };
    return this.myInfo;
  }

  bumpMyInfoVersion() {
    const myInfo = this.ensureMyInfo();
    myInfo.version = (myInfo.version || 0) + 1;
    myInfo.lastUpdate = { seconds: Math.floor(Date.now() / 1000), nanos: 0 };
  }

  setPublicServerFlag(isPublicServer) {
    const myInfo = this.ensureMyInfo();
    const next = !!isPublicServer;
    const prev = !!(myInfo.featureFlag && myInfo.featureFlag.isPublicServer);
    myInfo.featureFlag = { ...myInfo.featureFlag, isPublicServer: next };
    if (next !== prev) this.bumpMyInfoVersion();
  }

  setPureP2PMode(enabled) {
    const next = !!enabled;
    if (next === this.pureP2PMode) return;
    this.pureP2PMode = next;
    const myInfo = this.ensureMyInfo();
    myInfo.featureFlag = { ...myInfo.featureFlag, avoidRelayData: this.pureP2PMode };
    this.bumpMyInfoVersion();
  }

  isPureP2PMode() {
    return !!this.pureP2PMode;
  }

  _getPeersMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peersByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peersByGroup.set(k, m);
    }
    return m;
  }

  _getPeerInfosMap(groupKey, create = false) {
    const k = String(groupKey || '');
    let m = this.peerInfosByGroup.get(k);
    if (!m && create) {
      m = new Map();
      this.peerInfosByGroup.set(k, m);
    }
    return m;
  }

  _getSession(groupKey, peerId, create = false) {
    const now = Date.now();
    if (now - this.lastSessionCleanup > Math.max(30_000, Math.min(this.sessionTtlMs / 2, 120_000))) {
      this.cleanupSessions(now);
    }
    const gk = String(groupKey || '');
    let group = this.routeSessions.get(gk);
    if (!group && create) {
      group = new Map();
      this.routeSessions.set(gk, group);
    }
    if (!group) return null;

    let session = group.get(peerId);
    if (!session && create) {
      session = {
        mySessionId: null,
        dstSessionId: null,
        weAreInitiator: false,
        peerInfoVerMap: new Map(),
        foreignNetVer: 0,
        lastTouch: now,
        lastConnBitmapSig: null,
      };
      group.set(peerId, session);
    }
    if (session) session.lastTouch = now;
    return session;
  }

  cleanupSessions(nowTs = Date.now()) {
    this.lastSessionCleanup = nowTs;
    for (const [groupKey, group] of this.routeSessions.entries()) {
      for (const [peerId, session] of group.entries()) {
        if (nowTs - (session.lastTouch || 0) > this.sessionTtlMs) {
          group.delete(peerId);
        }
      }
      if (group.size === 0) this.routeSessions.delete(groupKey);
    }
  }

  onRouteSessionAck(groupKey, peerId, theirSessionId, weAreInitiator) {
    const session = this._getSession(groupKey, peerId, true);
    if (!session) return;

    if (session.dstSessionId !== theirSessionId) {
      // 对端重启或重建了会话，清空增量推送记录，确保重新同步全量
      session.peerInfoVerMap.clear();
      session.foreignNetVer = 0;
      session.lastConnBitmapSig = null;
    }

    session.dstSessionId = theirSessionId;
    if (typeof weAreInitiator === 'boolean') {
      session.weAreInitiator = weAreInitiator;
    }

    // 收到 ack：清除在途标记；若期间又有变更，立即补发一次
    const ws = this.getPeerWs(groupKey, peerId);
    if (ws) {
      const st = this._routeSyncState(ws);
      st.inFlight = false;
      st.lastCompletedAt = Date.now();
      if (st.dirty || st.forceFull) this.flushRouteUpdate(ws, this.types);
    }
  }

  /**
   * 每条连接的路由同步状态。
   * 该对象挂在 ws 上，并随 attachment 持久化（见 relay_room._updateAttachment），
   * 因此 Durable Object 休眠唤醒后仍能判断是否有请求在途。
   */
  _routeSyncState(ws) {
    if (!ws.routeSync) {
      ws.routeSync = {
        inFlight: false,
        dirty: false,
        forceFull: false,
        sentAt: 0,
        lastCompletedAt: 0,
      };
    }
    return ws.routeSync;
  }

  /**
   * 标记该连接需要同步路由，并尽量立即发送。
   * 已有请求在途时只置脏标记，等 ack 回来再合并发送，避免重复全量。
   */
  queueRouteUpdate(ws, types, opts = {}) {
    if (!ws || ws.readyState !== WS_OPEN) return false;
    const st = this._routeSyncState(ws);
    st.dirty = true;
    if (opts.forceFull) st.forceFull = true;
    return st.inFlight ? false : this.flushRouteUpdate(ws, types);
  }

  /**
   * 发送一次路由同步。
   * 发送失败（如被出向限速丢弃）时保留脏标记，等 alarm 补偿重试。
   */
  flushRouteUpdate(ws, types) {
    if (!ws || ws.readyState !== WS_OPEN || !ws.peerId) return false;
    const st = this._routeSyncState(ws);
    if (st.inFlight) return false;

    const forceFull = st.forceFull;
    st.dirty = false;
    st.forceFull = false;

    let sent = false;
    try {
      sent = this.pushRouteUpdateTo(ws.peerId, ws, types, { forceFull });
    } catch (e) {
      if (this.config.debug) {
        console.warn(`[RouteUpdate] flush to ${ws.peerId} failed: ${e.message}`);
      }
    }

    if (!sent) {
      st.dirty = true;
      st.forceFull = st.forceFull || forceFull;
      return false;
    }

    st.inFlight = true;
    st.sentAt = Date.now();
    return true;
  }

  /** alarm 周期调用：补发脏标记，超时未 ack 的请求强制全量重发 */
  maintainRouteSync(nowTs = Date.now()) {
    if (!this.types) return;

    for (const [, peers] of this.peersByGroup.entries()) {
      for (const [, ws] of peers.entries()) {
        if (!ws || ws.readyState !== WS_OPEN) continue;
        const st = this._routeSyncState(ws);

        if (st.inFlight && nowTs - st.sentAt > ROUTE_SYNC_TIMEOUT_MS) {
          if (this.config.debug) {
            console.warn(`[RouteUpdate] ack timeout peer=${ws.peerId}, retry full`);
          }
          st.inFlight = false;
          st.forceFull = true;
          st.dirty = true;
        }

        if (!st.inFlight && (st.dirty || st.forceFull)) {
          this.flushRouteUpdate(ws, this.types);
        }
      }
    }
  }

  /** @returns {boolean} 是否成功注册（超过上限时返回 false） */
  addPeer(peerId, ws) {
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    const peers = this._getPeersMap(groupKey, true);
    if (!peers.has(peerId) && peers.size >= this.config.maxPeersPerGroup) {
      return false;
    }
    peers.set(peerId, ws);
    return true;
  }

  /** @returns {boolean} 该连接此前是否确实登记在此 group 中 */
  removePeer(ws) {
    const peerId = ws && ws.peerId;
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    if (!peerId) return false;

    this.globalState.cleanPeerAndSubPeers(groupKey, peerId);

    const peers = this._getPeersMap(groupKey, false);
    const wasPresent = !!(peers && peers.get(peerId) === ws);
    if (wasPresent) peers.delete(peerId);

    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) infos.delete(peerId);

    const sessions = this.routeSessions.get(groupKey);
    if (sessions) {
      sessions.delete(peerId);
      if (sessions.size === 0) this.routeSessions.delete(groupKey);
    }

    const reported = this.reportedEdges.get(groupKey);
    if (reported) reported.delete(peerId);

    if (peers && peers.size === 0) {
      this.peersByGroup.delete(groupKey);
      this.peerInfosByGroup.delete(groupKey);
      this.bitmapCache.delete(groupKey);
    }
    return wasPresent;
  }

  getPeerWs(peerId, groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? peers.get(peerId) : undefined;
  }

  listPeerIdsInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.keys()) : [];
  }

  listPeersInGroup(groupKey) {
    const peers = this._getPeersMap(groupKey, false);
    return peers ? Array.from(peers.entries()) : [];
  }

  updatePeerInfo(groupKey, peerId, info) {
    const infos = this._getPeerInfosMap(groupKey, true);
    infos.set(peerId, info);
  }

  /**
   * 记录某个客户端上报的连接关系（SyncRouteInfoRequest.connBitmap）。
   *
   * 客户端打洞成功后，会在下一次路由同步里把自己的 conn_map 发上来（该行对应的 bit 置 1）。
   * 服务端必须把这些边汇总进全局位图，否则路由表会永远认为「只能经中继」，
   * 即使直连已经建好，数据仍然走中继。
   *
   * @returns {boolean} 汇总结果是否发生变化（变化时需要立即向其它节点广播）
   */
  recordConnBitmap(groupKey, fromPeerId, connBitmap) {
    if (!connBitmap || !Array.isArray(connBitmap.peerIds) || !connBitmap.bitmap) return false;

    const peerIds = connBitmap.peerIds.map((item) => Number(item && item.peerId));
    const n = peerIds.length;
    if (n === 0) return false;

    const bytes = connBitmap.bitmap;
    const edges = new Set();
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < n; j++) {
        if (i === j) continue;
        const bitIdx = i * n + j;
        const byte = bytes[bitIdx >>> 3];
        if (byte === undefined) continue;
        if ((byte & (1 << (bitIdx & 7))) === 0) continue;
        const a = peerIds[i];
        const b = peerIds[j];
        if (!Number.isFinite(a) || !Number.isFinite(b)) continue;
        edges.add(a < b ? `${a}:${b}` : `${b}:${a}`);
      }
    }

    const k = String(groupKey || '');
    let group = this.reportedEdges.get(k);
    if (!group) {
      group = new Map();
      this.reportedEdges.set(k, group);
    }

    const prev = group.get(fromPeerId);
    const changed = !prev || prev.edges.size !== edges.size
      || Array.from(edges).some((e) => !prev.edges.has(e));

    group.set(fromPeerId, { edges, ts: Date.now() });
    return changed;
  }

  /**
   * 生成连接位图并缓存。
   *
   * 语义与官方实现一致：n×n 位，bit(i, j) = 1 表示 peer_ids[i] 与 peer_ids[j] 直连。
   *
   * 位图内容 = 服务器与各节点的直连边 + 客户端上报的直连边（打洞结果）。
   * 客户端看到「自己与对端未直连」才会去尝试 GetIpList 打洞；
   * 打洞成功后必须把这条边反馈给它，否则它会一直把流量交给中继。
   *
   * EASYTIER_FULL_MESH=1 时退化为全互联（复现原版行为，仅用于排查）。
   */
  _getConnBitmap(groupKey, relevantPeers) {
    const n = relevantPeers.length;
    const size = Math.ceil((n * n) / 8);

    if (this.fullMesh) {
      const sig = `mesh|${relevantPeers.join(',')}`;
      const cached = this.bitmapCache.get(groupKey);
      if (cached && cached.sig === sig) return cached.bitmap;

      const bitmap = new Uint8Array(size).fill(0xff);
      const excessBits = size * 8 - n * n;
      if (excessBits > 0) bitmap[size - 1] &= 0xff >>> excessBits;
      this.bitmapCache.set(groupKey, { sig, bitmap });
      return bitmap;
    }

    const indexOf = new Map();
    for (let i = 0; i < n; i++) indexOf.set(Number(relevantPeers[i]), i);

    const bitmap = new Uint8Array(size);
    const edges = [];
    const setEdge = (from, to) => {
      if (from === undefined || to === undefined || from === to) return;
      const bitIdx = from * n + to;
      bitmap[bitIdx >>> 3] |= 1 << (bitIdx & 7);
      edges.push(from < to ? `${from}:${to}` : `${to}:${from}`);
    };

    // 服务器与所有直连节点互连
    const myIdx = indexOf.get(MY_PEER_ID);
    for (const [pid] of this.listPeersInGroup(groupKey)) {
      const peerIdx = indexOf.get(Number(pid));
      if (peerIdx === undefined) continue;
      setEdge(myIdx, peerIdx);
      setEdge(peerIdx, myIdx);
    }

    // 汇总各客户端上报的直连边（打洞成功后的结果）。
    // 不做超时淘汰：客户端只在 conn_map 变化时才上报（其余时间是 conn_info: None），
    // 若按时效淘汰，直连明明还在也会被误删，路由会退回中继。
    // 边的生命周期由会话决定：节点断开时 removePeer 会清掉，连接断开时客户端会重新上报。
    const reported = this.reportedEdges.get(String(groupKey || ''));
    if (reported) {
      for (const entry of reported.values()) {
        if (!entry) continue;
        for (const key of entry.edges) {
          const sep = key.indexOf(':');
          const a = indexOf.get(Number(key.slice(0, sep)));
          const b = indexOf.get(Number(key.slice(sep + 1)));
          if (a === undefined || b === undefined) continue;
          setEdge(a, b);
          setEdge(b, a);
        }
      }
    }

    const sig = `${relevantPeers.join(',')}|${edges.sort().join(',')}`;
    const cached = this.bitmapCache.get(groupKey);
    if (cached && cached.sig === sig) return cached.bitmap;

    this.bitmapCache.set(groupKey, { sig, bitmap });
    return bitmap;
  }

  /**
   * 单调递增的路由版本号。
   * 以秒级时间戳为基准，重启后也不会比客户端已缓存的版本更小；
   * 同一秒内的多次变更靠 +1 保证严格递增。
   */
  _nextVersion() {
    const nowSec = Math.floor(Date.now() / 1000) % 2_000_000_000;
    const next = Math.max(nowSec, this.globalNetworkVersion + 1);
    this.globalNetworkVersion = next > 0xFFFFFFFF ? 0xFFFFFFFF : next;
    return this.globalNetworkVersion;
  }

  broadcastRouteUpdate(types, groupKey, excludePeerId, opts = {}) {
    const forceFull = opts.forceFull !== undefined ? !!opts.forceFull : true;
    const groups = groupKey !== undefined
      ? [[String(groupKey), this._getPeersMap(groupKey, false)]]
      : Array.from(this.peersByGroup.entries());

    for (const [, peers] of groups) {
      if (!peers) continue;
      for (const [peerId, ws] of peers.entries()) {
        if (peerId === excludePeerId) continue;
        if (ws.readyState !== WS_OPEN) continue;
        try {
          this.queueRouteUpdate(ws, types, { forceFull });
        } catch (e) {
          if (this.config.debug) {
            console.warn(`[RouteUpdate] push to ${peerId} failed: ${e.message}`);
          }
        }
      }
    }
  }

  pushRouteUpdateTo(targetPeerId, ws, types, opts = {}) {
    if (!types) throw new Error('PeerManager types not set');

    const forceFull = !!opts.forceFull;
    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';
    const session = this._getSession(groupKey, targetPeerId, true);
    if (!session) return false;

    const myInfo = this.ensureMyInfo();
    if (!ws.serverSessionId) ws.serverSessionId = randomU64String();
    session.mySessionId = ws.serverSessionId;
    const forceFullLocal = forceFull || !session.dstSessionId;

    // 收集所有已知 peer（直连 + 路由信息 + 全局映射中的子设备）
    const allPeers = new Set(this.listPeerIdsInGroup(groupKey));
    const infos = this._getPeerInfosMap(groupKey, false);
    if (infos) {
      for (const pid of infos.keys()) allPeers.add(pid);
    }

    const knownSubPeerIds = new Set();
    const globalState = this.globalState.getPeerCenterState(groupKey);
    for (const peerInfo of globalState.globalPeerMap.values()) {
      if (!peerInfo.directPeers) continue;
      for (const subPeerId of Object.keys(peerInfo.directPeers)) {
        const id = Number(subPeerId);
        knownSubPeerIds.add(id);
        allPeers.add(id);
      }
    }

    allPeers.add(targetPeerId);
    const relevantPeers = [
      MY_PEER_ID,
      ...Array.from(allPeers)
        .filter((pid) => pid !== MY_PEER_ID)
        .sort((a, b) => Number(a) - Number(b)),
    ];
    const defaultNetLen = myInfo.networkLength || 24;

    const peerInfosItems = [];
    for (const pid of relevantPeers) {
      let info = pid === MY_PEER_ID ? myInfo : (infos ? infos.get(pid) : null);

      if (!info && pid !== MY_PEER_ID) {
        if (!knownSubPeerIds.has(pid)) continue;
        info = this._makeStubPeerInfo(pid, defaultNetLen);
      }
      if (!info) continue;

      const version = info.version || 1;
      const prev = forceFullLocal ? 0 : (session.peerInfoVerMap.get(pid) || 0);
      if (forceFullLocal || version > prev) {
        peerInfosItems.push(info);
        session.peerInfoVerMap.set(pid, version);
      }
    }

    let connBitmap = null;
    if (relevantPeers.length > 0) {
      const bitmap = this._getConnBitmap(groupKey, relevantPeers);
      // 签名必须包含位图内容：打洞成功后连接关系变化，但 peer 列表不变，
      // 若只用 peer 列表做签名，版本号不会递增，客户端会忽略这次更新。
      const sig = `${relevantPeers.join(',')}|${Array.from(bitmap).join('.')}`;

      if (forceFullLocal || sig !== session.lastConnBitmapSig) {
        this._nextVersion();
        session.lastConnBitmapSig = sig;
      }
      const version = this.globalNetworkVersion;

      connBitmap = {
        peerIds: relevantPeers.map((pid) => ({ peerId: pid, version })),
        bitmap,
        version,
      };
    }

    const foreignNetworkInfos = this._buildForeignNetworkInfos(session, allPeers);

    const reqPayload = {
      myPeerId: MY_PEER_ID,
      mySessionId: ws.serverSessionId,
      isInitiator: !!ws.weAreInitiator,
      peerInfos: peerInfosItems.length > 0 ? { items: peerInfosItems } : null,
      connBitmap,
      foreignNetworkInfos,
    };

    const reqBytes = types.SyncRouteInfoRequest.encode(reqPayload).finish();
    const rpcRequestBytes = types.RpcRequest.encode({ request: reqBytes, timeoutMs: 5000 }).finish();

    const rpcReqPacket = {
      fromPeer: MY_PEER_ID,
      toPeer: targetPeerId,
      transactionId: randomUint32(),
      descriptor: {
        domainName: ws.domainName || this.config.publicServerNetworkName,
        protoName: 'OspfRouteRpc',
        serviceName: 'OspfRouteRpc',
        methodIndex: this.config.ospfRouteMethodIndex,
      },
      body: rpcRequestBytes,
      isRequest: true,
      totalPieces: 1,
      pieceIdx: 0,
      traceId: 0,
      compressionInfo: { algo: 1, acceptedAlgo: 1 },
    };

    const rpcPacketBytes = types.RpcPacket.encode(rpcReqPacket).finish();
    return sendWithStats(ws, wrapPacket(createHeader, MY_PEER_ID, targetPeerId, PacketType.RpcReq, rpcPacketBytes, ws));
  }

  _makeStubPeerInfo(peerId, networkLength) {
    const c = this.config;
    return {
      peerId,
      version: 1,
      lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
      instId: makeInstId(),
      cost: 1,
      hostname: c.hostname,
      easytierVersion: c.easytierVersion,
      featureFlag: { isPublicServer: false, avoidRelayData: false, kcpInput: false, noRelayKcp: false },
      networkLength: Number(networkLength || c.networkLength || 24),
      peerRouteId: randomU64String(),
      groups: [],
      udpStunInfo: 1,
    };
  }

  _buildForeignNetworkInfos(session, allPeers) {
    const mode = this.config.handshakeMode;
    if (mode === 'same' || mode === 'same_network') return null;

    const version = session.foreignNetVer + 1;
    session.foreignNetVer = version;
    return {
      infos: [{
        key: {
          peerId: MY_PEER_ID,
          networkName: this.config.publicServerNetworkName,
        },
        value: {
          foreignPeerIds: Array.from(allPeers),
          lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
          version,
          // 官方 proto 要求这里必须是 32 字节
          networkSecretDigest: new Uint8Array(32),
          myPeerIdForThisNetwork: MY_PEER_ID,
        },
      }],
    };
  }
}
