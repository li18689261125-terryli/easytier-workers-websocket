/**
 * EasyTier RPC 处理
 *
 * 覆盖 PeerCenterRpc（全局 peer 映射）与 OspfRouteRpc（路由同步）两类服务。
 *
 * 关键修复：
 * - 构建 PeerCenter 响应时补齐 peerManager 参数（旧代码漏传导致 TypeError）
 * - 不再对外发送压缩后的 RPC 响应：旧实现用 gzip，却把 algo 标成 2（Zstd），
 *   客户端按 Zstd 解压必然失败。现在固定 algo=1（None）。
 * - 入站若真收到压缩体，仅做尽力而为的 gunzip 尝试，失败则丢弃并记录。
 *
 * @file rpc_handler.js
 */

import { MY_PEER_ID, PacketType } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String, sendWithStats } from './crypto.js';
import { gunzipMaybe } from './compress.js';
import { calcPeerCenterDigest, buildPeerCenterResponseMap } from './global_state.js';

// 把 transactionId 归一化成 protobufjs 可接受的形式
function toLongForProto(value) {
  if (value === null || value === undefined) return value;
  if (value && typeof value === 'object' && typeof value.low === 'number' && typeof value.high === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    try {
      const big = BigInt(value);
      return {
        low: Number(big & 0xffffffffn),
        high: Number((big >> 32n) & 0xffffffffn),
        unsigned: false,
      };
    } catch (_) {
      return value;
    }
  }
  if (typeof value === 'number') {
    return { low: value | 0, high: Math.floor(value / 4294967296), unsigned: false };
  }
  if (typeof value === 'bigint') {
    return {
      low: Number(value & 0xffffffffn),
      high: Number((value >> 32n) & 0xffffffffn),
      unsigned: false,
    };
  }
  return value;
}

function sendRpcResponse(ws, toPeerId, reqRpcPacket, types, responseBodyBytes) {
  if (!ws || ws.readyState !== 1) {
    return;
  }

  const rpcResponseBytes = types.RpcResponse.encode({
    response: responseBodyBytes,
    error: null,
    runtimeUs: 0,
  }).finish();

  const rpcRespPacket = {
    fromPeer: MY_PEER_ID,
    toPeer: toPeerId,
    transactionId: toLongForProto(reqRpcPacket.transactionId),
    descriptor: reqRpcPacket.descriptor,
    body: rpcResponseBytes,
    isRequest: false,
    totalPieces: 1,
    pieceIdx: 0,
    traceId: reqRpcPacket.traceId,
    // 只能声明 None；本实现不具备 Zstd 编码能力
    compressionInfo: { algo: 1, acceptedAlgo: 1 },
  };

  const rpcPacketBytes = types.RpcPacket.encode(rpcRespPacket).finish();
  const buf = wrapPacket(createHeader, MY_PEER_ID, toPeerId, PacketType.RpcResp, rpcPacketBytes, ws);
  sendWithStats(ws, buf);
}

/** 入站压缩体只有 algo > 1 时需要处理；不支持时丢弃该报文 */
function decompressInbound(packet, fromPeerId, config) {
  const algo = packet.compressionInfo && packet.compressionInfo.algo;
  if (!algo || algo <= 1) return true;
  try {
    packet.body = gunzipMaybe(packet.body);
    packet.compressionInfo.algo = 1;
    return true;
  } catch (e) {
    if (config.debug) {
      console.warn(`[rpc] decompress algo=${algo} from ${fromPeerId} failed: ${e.message}`);
    }
    return false;
  }
}

function isOspfRouteRpc(descriptor) {
  return (descriptor.serviceName === 'peer_rpc.OspfRouteRpc' || descriptor.serviceName === 'OspfRouteRpc')
    && (descriptor.protoName === 'peer_rpc'
      || descriptor.protoName === 'peer_rpc.OspfRouteRpc'
      || descriptor.protoName === 'OspfRouteRpc'
      || !descriptor.protoName);
}

function isPeerCenterRpc(descriptor) {
  return (descriptor.serviceName === 'peer_rpc.PeerCenterRpc' || descriptor.serviceName === 'PeerCenterRpc')
    && (descriptor.protoName === 'peer_rpc' || !descriptor.protoName);
}

export function handleRpcReq(ws, header, payload, ctx) {
  const { config, types, pm, globalState } = ctx;

  try {
    const rpcPacket = types.RpcPacket.decode(payload);

    if (!decompressInbound(rpcPacket, header.fromPeerId, config)) return;

    const descriptor = rpcPacket.descriptor || {};

    let innerReqBody = rpcPacket.body;
    try {
      const wrapper = types.RpcRequest.decode(rpcPacket.body);
      if (wrapper.request && wrapper.request.length > 0) {
        innerReqBody = wrapper.request;
      }
    } catch (_) {
      // 部分客户端不发外层 RpcRequest 包装，直接使用原始 body
    }

    const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';

    if (isPeerCenterRpc(descriptor)) {
      const state = globalState.getPeerCenterState(groupKey);

      if (descriptor.methodIndex === 0) {
        const req = types.ReportPeersRequest.decode(innerReqBody);

        // 不允许替其它节点上报，否则可以污染他人名下的直连信息
        if (req.myPeerId && Number(req.myPeerId) !== Number(header.fromPeerId)) {
          if (config.debug) {
            console.warn(`[rpc] rejected spoofed ReportPeersRequest myPeerId=${req.myPeerId} from=${header.fromPeerId}`);
          }
          return;
        }

        const peers = req.peerInfos || { directPeers: {} };

        // 限制单次上报的条目数，防止超大报文把全局表撑爆
        const directPeers = {};
        const entries = Object.entries(peers.directPeers || {});
        const limit = Math.min(entries.length, config.maxDirectPeersPerReport);
        for (let i = 0; i < limit; i++) {
          const [dstPeerId, info] = entries[i];
          directPeers[String(dstPeerId)] = {
            latencyMs: (info && typeof info.latencyMs === 'number') ? info.latencyMs : 0,
          };
        }

        const selfKey = String(req.myPeerId);
        if (!state.globalPeerMap.has(selfKey) && state.globalPeerMap.size >= config.maxPeerCenterEntries) {
          // 超出上限时淘汰最久未更新的条目，保证内存有界
          let oldestKey = null;
          let oldest = Infinity;
          for (const [k, v] of state.globalPeerMap.entries()) {
            const seen = v.lastSeen || 0;
            if (seen < oldest) {
              oldest = seen;
              oldestKey = k;
            }
          }
          if (oldestKey !== null) state.globalPeerMap.delete(oldestKey);
        }

        state.globalPeerMap.set(selfKey, { directPeers, lastSeen: Date.now() });

        // 旧代码这里漏传 pm，抛 TypeError 后整个 RPC 静默失败
        const snapshot = buildPeerCenterResponseMap(groupKey, state, pm);
        state.digest = calcPeerCenterDigest(snapshot);

        sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, types.ReportPeersResponse.encode({}).finish());
        return;
      }

      if (descriptor.methodIndex === 1) {
        const req = types.GetGlobalPeerMapRequest.decode(innerReqBody);
        const reqDigest = req.digest !== undefined && req.digest !== null ? String(req.digest) : '0';
        if (reqDigest === state.digest && reqDigest !== '0') {
          sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, types.GetGlobalPeerMapResponse.encode({}).finish());
          return;
        }

        const snapshot = buildPeerCenterResponseMap(groupKey, state, pm);
        state.digest = calcPeerCenterDigest(snapshot);

        const respBytes = types.GetGlobalPeerMapResponse.encode({
          globalPeerMap: snapshot,
          digest: state.digest,
        }).finish();
        sendRpcResponse(ws, header.fromPeerId, rpcPacket, types, respBytes);
        return;
      }

      if (config.debug) console.log(`[rpc] unhandled PeerCenterRpc methodIndex=${descriptor.methodIndex}`);
      return;
    }

    if (isOspfRouteRpc(descriptor)) {
      const req = types.SyncRouteInfoRequest.decode(innerReqBody);
      if (descriptor.methodIndex === 0 || descriptor.methodIndex === 1) {
        handleSyncRouteInfo(ws, header.fromPeerId, rpcPacket, req, ctx);
        return;
      }
      if (config.debug) console.log(`[rpc] unhandled OspfRouteRpc methodIndex=${descriptor.methodIndex}`);
      return;
    }

    // 未处理的服务：始终记录。RPC 数量极小，而这里正是客户端打洞信令
    // （peer_rpc.DirectConnectorRpc.GetIpList / SendUdpHolePunchPacket）会落到的分支，
    // 常开日志便于判断客户端到底有没有发起直连尝试。
    console.log(
      `[rpc] unhandled service=${descriptor.serviceName} proto=${descriptor.protoName} `
      + `method=${descriptor.methodIndex} from=${header.fromPeerId} to=${header.toPeerId}`,
    );
  } catch (e) {
    console.error('RPC Decode error:', e);
  }
}

export function handleRpcResp(ws, header, payload, ctx) {
  const { config, types, pm } = ctx;

  try {
    const rpcPacket = types.RpcPacket.decode(payload);
    if (!decompressInbound(rpcPacket, header.fromPeerId, config)) return;

    const descriptor = rpcPacket.descriptor || {};
    let rpcRespBody = rpcPacket.body;

    try {
      const decoded = types.RpcResponse.decode(rpcRespBody);
      rpcRespBody = decoded.response || rpcRespBody;
    } catch (e) {
      if (config.debug) {
        console.warn(`[rpc] RpcResponse wrapper decode failed from ${header.fromPeerId}: ${e.message}`);
      }
    }

    // 只关心 OspfRouteRpc 的 SyncRouteInfoResponse（用于确认对端会话）
    if (isOspfRouteRpc(descriptor)) {
      try {
        const resp = types.SyncRouteInfoResponse.decode(rpcRespBody);
        if (resp && resp.sessionId && ws && ws.groupKey !== undefined) {
          pm.onRouteSessionAck(ws.groupKey, header.fromPeerId, resp.sessionId, ws.weAreInitiator);
        }
      } catch (e) {
        if (config.debug) {
          console.warn(`[rpc] SyncRouteInfoResponse decode failed from ${header.fromPeerId}: ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.error('RPC Resp Decode error:', e);
  }
}

function handleSyncRouteInfo(ws, fromPeerId, reqRpcPacket, syncReq, ctx) {
  const { config, types, pm, globalState } = ctx;
  const groupKey = ws && ws.groupKey ? String(ws.groupKey) : '';

  if (!ws.serverSessionId) ws.serverSessionId = randomU64String();

  if (syncReq && typeof syncReq.isInitiator === 'boolean') {
    ws.weAreInitiator = !syncReq.isInitiator;
  }
  pm.onRouteSessionAck(groupKey, fromPeerId, syncReq.mySessionId, ws.weAreInitiator);

  // 记录该客户端上报的连接关系（打洞成功后它会把自己的直连边发上来）
  const bitmapChanged = pm.recordConnBitmap(groupKey, fromPeerId, syncReq.connBitmap);

  let hasNewPeers = false;
  let hasSubPeers = false;

  const items = (syncReq.peerInfos && Array.isArray(syncReq.peerInfos.items))
    ? syncReq.peerInfos.items.slice(0, config.maxPeersPerGroup)
    : [];

  const directPeerSet = new Set(pm.listPeerIdsInGroup(groupKey));

  for (const info of items) {
    if (!info || !info.peerId) continue;

    // 客户端没上报 STUN 信息时给一个 P2P 友好的默认值（FullCone）
    if (!info.udpStunInfo) info.udpStunInfo = 3;

    if (info.peerId === MY_PEER_ID) {
      pm.updatePeerInfo(groupKey, info.peerId, info);
      continue;
    }

    const infos = pm._getPeerInfosMap(groupKey, false);
    if (!infos || !infos.has(info.peerId)) hasNewPeers = true;

    pm.updatePeerInfo(groupKey, info.peerId, info);

    if (!directPeerSet.has(info.peerId)) hasSubPeers = true;
  }

  if (hasSubPeers) {
    const state = globalState.getPeerCenterState(groupKey);
    const directPeers = {};
    for (const info of items) {
      if (info && info.peerId && info.peerId !== MY_PEER_ID && info.peerId !== fromPeerId) {
        directPeers[String(info.peerId)] = { latencyMs: 10 };
      }
    }
    if (Object.keys(directPeers).length > 0) {
      state.globalPeerMap.set(String(fromPeerId), { directPeers, lastSeen: Date.now() });
    }
  }

  const respBytes = types.SyncRouteInfoResponse.encode({
    isInitiator: !syncReq.isInitiator,
    sessionId: ws.serverSessionId,
  }).finish();

  try {
    sendRpcResponse(ws, fromPeerId, reqRpcPacket, types, respBytes);
  } catch (e) {
    console.error(`Failed to send SyncRouteInfoResponse to ${fromPeerId}: ${e.message}`);
    return;
  }

  // 回推当前路由（与官方节点行为一致）
  try {
    pm.queueRouteUpdate(ws, types, { forceFull: true });
  } catch (e) {
    if (config.debug) console.warn(`[rpc] push route update to ${fromPeerId} failed: ${e.message}`);
  }

  if (hasNewPeers || hasSubPeers || bitmapChanged) {
    try {
      pm.broadcastRouteUpdate(types, groupKey, fromPeerId, { forceFull: true });
    } catch (e) {
      if (config.debug) console.warn(`[rpc] broadcast route update failed: ${e.message}`);
    }
  }
}
