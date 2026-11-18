/**
 * EasyTier 基础消息处理器
 *
 * 处理握手、心跳应答与报文转发。
 * 旧实现中模块级的 networkDigestRegistry / networkGroups 仅在 /admin 端点使用，
 * 且存在跨 Durable Object 串台与泄漏问题，现已整体移除。
 *
 * @file basic_handlers.js
 */

import { MAGIC, VERSION, MY_PEER_ID, PacketType, CloseCode } from './constants.js';
import { createHeader } from './packet.js';
import { wrapPacket, randomU64String, sendWithStats } from './crypto.js';
import { networkSecretDigest, timingSafeEqual } from './network_secret.js';

const WS_OPEN = 1;

function toHex(bytes) {
  let out = '';
  for (let i = 0; i < bytes.length; i++) {
    out += bytes[i].toString(16).padStart(2, '0');
  }
  return out;
}

/**
 * @param {object} ws
 * @param {object} header
 * @param {Uint8Array} payload
 * @param {object} ctx { config, types, pm, globalState }
 * @returns {boolean} 握手是否成功
 */
function handleHandshake(ws, header, payload, ctx) {
  const { config, types, pm } = ctx;

  let req;
  try {
    req = types.HandshakeRequest.decode(payload);
  } catch (e) {
    ws.close(CloseCode.Unauthorized, 'Bad handshake');
    return false;
  }

  if (req.magic !== MAGIC) {
    ws.close(CloseCode.Unauthorized, 'Magic mismatch');
    return false;
  }

  // 拒绝客户端自称服务端 ID 或使用无效 ID
  if (!req.myPeerId || req.myPeerId === MY_PEER_ID) {
    ws.close(CloseCode.Unauthorized, 'Invalid peer id');
    return false;
  }

  const clientNetworkName = req.networkName || '';
  if (clientNetworkName.length > 64) {
    ws.close(CloseCode.Unauthorized, 'Network name too long');
    return false;
  }

  // 私有模式：配置了网络名就只接受同名客户端
  if (config.networkName && clientNetworkName !== config.networkName) {
    ws.close(CloseCode.NetworkRejected, 'Network name mismatch');
    return false;
  }

  // 密钥摘要必须是固定 32 字节（或完全不提供），否则无法作为稳定的分组标识，
  // 同时避免任意长度的摘要进入 groupKey 与日志
  if (req.networkSecretDigrest && req.networkSecretDigrest.length !== 32) {
    ws.close(CloseCode.Unauthorized, 'Invalid network secret digest');
    return false;
  }

  // 配置了网络密钥就强制校验：只有使用相同密钥的客户端才能加入同名网络。
  // 客户端上报的是 network_secret_digest（与 EasyTier 客户端同算法），
  // 未配置密钥时保持原行为，仅把摘要当作分组标识。
  if (config.networkSecret) {
    const expected = networkSecretDigest(config.networkName, config.networkSecret);
    if (!timingSafeEqual(req.networkSecretDigrest, expected)) {
      if (config.debug) {
        console.warn(`[Handshake] network secret mismatch peer=${req.myPeerId} network="${clientNetworkName}"`);
      }
      ws.close(CloseCode.NetworkRejected, 'Network secret mismatch');
      return false;
    }
  }

  const isPublicServer = !config.networkName;
  const serverNetworkName = config.networkName || config.publicServerNetworkName;
  const digestHex = toHex(req.networkSecretDigrest || new Uint8Array(0));

  ws.domainName = clientNetworkName;
  ws.groupKey = `${clientNetworkName}:${digestHex}`;
  ws.peerId = req.myPeerId;

  if (!pm.addPeer(req.myPeerId, ws)) {
    ws.close(1013, 'Too many peers');
    return false;
  }

  // 先回握手，再推路由；同一个 WebSocket 上消息有序，客户端会先看到握手响应
  const respPayload = {
    magic: MAGIC,
    myPeerId: MY_PEER_ID,
    version: VERSION,
    networkName: serverNetworkName,
    // 注意：官方 proto 里这个字段拼写就是 Digrest
    networkSecretDigrest: new Uint8Array(32),
  };
  const respBuffer = types.HandshakeRequest.encode(respPayload).finish();
  sendWithStats(ws, wrapPacket(createHeader, MY_PEER_ID, req.myPeerId, PacketType.HandShake, respBuffer, ws));

  pm.updatePeerInfo(ws.groupKey, req.myPeerId, {
    peerId: req.myPeerId,
    version: 1,
    lastUpdate: { seconds: Math.floor(Date.now() / 1000), nanos: 0 },
    instId: { part1: 0, part2: 0, part3: 0, part4: 0 },
    networkLength: Number(config.networkLength || 24),
  });
  pm.setPublicServerFlag(isPublicServer);

  if (!ws.serverSessionId) ws.serverSessionId = randomU64String();
  if (ws.weAreInitiator === undefined) ws.weAreInitiator = false;

  pm.queueRouteUpdate(ws, types, { forceFull: true });
  // 广播时排除新节点，避免它同一轮收到两份全量路由
  pm.broadcastRouteUpdate(types, ws.groupKey, req.myPeerId, { forceFull: true });

  if (config.debug) {
    console.log(`[Handshake] peer=${req.myPeerId} network="${clientNetworkName}" group=${ws.groupKey}`);
  }
  return true;
}

function handlePing(ws, header, payload, ctx) {
  const msg = wrapPacket(createHeader, MY_PEER_ID, header.fromPeerId, PacketType.Pong, payload, ws);
  sendWithStats(ws, msg);
}

function handleForwarding(sourceWs, header, fullMessage, ctx) {
  const { config, types, pm } = ctx;

  if (config.disableRelay && header.packetType === PacketType.Data) {
    return; // 纯 P2P 模式：不中转数据面
  }

  const targetWs = pm.getPeerWs(header.toPeerId, sourceWs && sourceWs.groupKey);
  if (!targetWs || targetWs.readyState !== WS_OPEN) {
    return;
  }

  // 禁止跨网络组转发
  if (sourceWs && targetWs.groupKey && sourceWs.groupKey && targetWs.groupKey !== sourceWs.groupKey) {
    return;
  }

  try {
    sendWithStats(targetWs, fullMessage);
  } catch (e) {
    pm.removePeer(targetWs);
    try {
      pm.broadcastRouteUpdate(types, sourceWs && sourceWs.groupKey, null, { forceFull: true });
    } catch (_) {
      // 广播失败不影响当前处理
    }
  }
}

export { handleHandshake, handlePing, handleForwarding };
