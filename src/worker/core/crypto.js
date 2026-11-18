/**
 * 报文封装工具
 *
 * 说明：本 Worker 只做明文中继（协议模拟），不参与 EasyTier 的密钥协商，
 * 因此这里只保留实际用到的两个函数，删除了原先整套未被调用的
 * SipHash/AES-GCM/密钥派生实现。
 *
 * @file crypto.js
 */

export function randomU64String() {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  let value = 0n;
  for (let i = 0; i < 8; i++) {
    value = (value << 8n) | BigInt(bytes[i]);
  }
  return value.toString();
}

/**
 * 拼接 16 字节头与负载。
 *
 * @param {Function} createHeader 头部构造函数（由上层注入，避免循环依赖）
 * @param {number} fromPeerId
 * @param {number} toPeerId
 * @param {number} packetType
 * @param {Uint8Array} payload
 * @param {object} ws 目标连接，读取其 relayFlags（延迟优先标志位）
 */
export function wrapPacket(createHeader, fromPeerId, toPeerId, packetType, payload, ws) {
  const body = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  const flags = (ws && ws.relayFlags) || 0;
  const headerBuf = createHeader(fromPeerId, toPeerId, packetType, body.length, flags);

  const out = new Uint8Array(headerBuf.length + body.length);
  out.set(headerBuf, 0);
  out.set(body, headerBuf.length);
  return out;
}

/**
 * 出向 1 秒滚动窗口限速：同时限制字节数与帧数。
 *
 * 超限返回 false，由调用方直接丢弃该报文（不断连接）。
 * 目的是防止慢客户端（ws.send 缓冲暴涨）或广播风暴把 Durable Object 的内存拖爆。
 */
function allowOutbound(ws, bytes) {
  const limit = ws.outLimit;
  if (!limit) return true;

  const now = Date.now();
  if (!ws.outWindowAt || now - ws.outWindowAt >= 1000) {
    ws.outWindowAt = now;
    ws.outBytes = 0;
    ws.outFrames = 0;
  }

  if (ws.outBytes + bytes > limit.bytesPerSecond) return false;
  if (ws.outFrames + 1 > limit.framesPerSecond) return false;

  ws.outBytes += bytes;
  ws.outFrames += 1;
  return true;
}

/**
 * 发送报文并累计出向流量。
 *
 * ws.stats / ws.outLimit 由 relay_room 在连接建立（或休眠恢复）时初始化；
 * 被限速丢弃的报文不计数，也不发送。
 *
 * @returns {boolean} 是否真正写出
 */
export function sendWithStats(ws, data) {
  let bytes = 0;
  if (typeof data === 'string') bytes = new TextEncoder().encode(data).length;
  else if (data && data.byteLength !== undefined) bytes = data.byteLength;

  if (!allowOutbound(ws, bytes)) return false;

  try {
    ws.stats.msgsOut += 1;
    ws.stats.bytesOut += bytes;
  } catch (_) {
    // 没有统计对象时照常发送
  }
  return ws.send(data);
}
