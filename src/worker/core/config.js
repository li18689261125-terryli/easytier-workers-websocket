/**
 * 统一配置加载
 *
 * 原实现里各处直接读 process.env，有两个问题：
 * 1. WS_PATH 未配置时会拼出 "/undefined"（`'/' + undefined` 是 truthy，`|| '/ws'` 永远不生效）；
 * 2. 配置散落各处，无法按 Durable Object 实例隔离或覆写。
 *
 * 现在改为 RelayRoom 构造时解析一次，向下传递。
 *
 * @file config.js
 */

import { HEADER_SIZE } from './constants.js';

function toInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

export function loadConfig(env = {}) {
  const rawPath = typeof env.WS_PATH === 'string' ? env.WS_PATH.trim() : '';
  // 空值（或只填 "/"）表示不限制路径：随便填一个链接就能连上
  const anyPath = rawPath === '' || rawPath === '/';
  const normalized = '/' + rawPath.replace(/^\/+/, '').replace(/\/+$/, '');

  return {
    // WebSocket 入口路径；空字符串表示任意路径都接受
    wsPath: anyPath ? '' : normalized,

    heartbeatIntervalMs: toInt(env.EASYTIER_HEARTBEAT_INTERVAL, 10_000),
    connectionTimeoutMs: toInt(env.EASYTIER_CONNECTION_TIMEOUT, 25_000),
    // 握手超时：超过该时间仍未完成握手（peerId 未确定）的连接直接关闭，
    // 避免匿名半开连接长期占用 DO 资源
    handshakeTimeoutMs: toInt(env.EASYTIER_HANDSHAKE_TIMEOUT, 10_000),

    // 私有网络模式：非空时只接受同名网络
    networkName: typeof env.EASYTIER_NETWORK_NAME === 'string' ? env.EASYTIER_NETWORK_NAME.trim() : '',
    // 网络密钥：配置后强制校验客户端上报的 network_secret_digest，
    // 只有使用相同密钥的客户端才能加入（建议用 wrangler secret 保存）
    networkSecret: typeof env.EASYTIER_NETWORK_SECRET === 'string' ? env.EASYTIER_NETWORK_SECRET : '',
    publicServerNetworkName: env.EASYTIER_PUBLIC_SERVER_NETWORK_NAME || 'public_server',
    networkLength: toInt(env.EASYTIER_NETWORK_LENGTH, 24),
    easytierVersion: env.EASYTIER_VERSION || 'Easytier-Workers-Websocket',
    hostname: env.EASYTIER_HOSTNAME || 'Easytier-Workers-Websocket',

    latencyFirst: env.EASYTIER_LATENCY_FIRST === '1',
    // 纯 P2P 模式：只做控制面转发，不转发 Data 包
    disableRelay: env.EASYTIER_DISABLE_RELAY === '1',
    // 全互联位图：让客户端尝试 P2P 打洞
    // 默认关闭（星型，保证流量走中继）；置 1 则恢复原版的全互联行为
    fullMesh: env.EASYTIER_FULL_MESH === '1',

    sessionTtlMs: toInt(env.EASYTIER_SESSION_TTL_MS, 180_000),
    peerCenterTtlMs: toInt(env.EASYTIER_PEER_CENTER_TTL_MS, 180_000),
    ospfRouteMethodIndex: toInt(env.EASYTIER_OSPF_ROUTE_METHOD_INDEX, 1),
    handshakeMode: String(env.EASYTIER_HANDSHAKE_MODE || 'foreign').toLowerCase(),

    // 防护上限
    maxMessageBytes: toInt(env.EASYTIER_MAX_MESSAGE_BYTES, 1024 * 1024),
    maxPeersPerGroup: toInt(env.EASYTIER_MAX_PEERS_PER_GROUP, 512),
    // 单连接吞吐上限（字节/秒），超出部分丢弃而不断开连接
    maxBytesPerSecond: toInt(env.EASYTIER_MAX_BYTES_PER_SEC, 16 * 1024 * 1024),
    // 出向 1 秒窗口上限：字节数与帧数双限，超限报文直接丢弃，避免慢客户端把 DO 拖垮
    maxOutBytesPerSecond: toInt(env.EASYTIER_MAX_OUT_BYTES_PER_SEC, 32 * 1024 * 1024),
    maxOutFramesPerSecond: toInt(env.EASYTIER_MAX_OUT_FRAMES_PER_SEC, 4096),
    // 单次上报的直连节点数上限，以及 PeerCenter 全局表条目上限
    maxDirectPeersPerReport: toInt(env.EASYTIER_MAX_DIRECT_PEERS, 256),
    maxPeerCenterEntries: toInt(env.EASYTIER_MAX_PEER_CENTER_ENTRIES, 4096),

    debug: env.EASYTIER_DEBUG === '1',
  };
}

/**
 * 判断请求路径是否命中 WebSocket 入口。
 * wsPath 为空表示不限制路径（任意链接都可直连）。
 */
export function matchesWsPath(config, pathname) {
  if (!config.wsPath) return true;
  const path = pathname.replace(/\/+$/, '') || '/';
  return path === config.wsPath;
}

/** 合法的 Cloudflare 地域提示（写错会被静默忽略，所以要在自检里拦下） */
const LOCATION_HINTS = new Set(['wnam', 'enam', 'sam', 'weur', 'eeur', 'apac', 'oc', 'afr', 'me']);

/** 数值型环境变量：留空视为未配置，非数字或非正数视为写错 */
function invalidIntEnv(env, key) {
  const raw = env[key];
  if (raw === undefined || raw === null || raw === '') return false;
  const n = Number(raw);
  return !Number.isFinite(n) || n <= 0 || Math.floor(n) !== n;
}

/**
 * 配置自检：返回错误列表，空数组表示配置可用。
 *
 * 只校验"配错必然导致故障或静默失效"的项，供 /healthz 在启动前暴露问题，
 * 避免线上只看到 /healthz 永远是 ok、实际却连不上。
 *
 * @param {object} config loadConfig() 的结果
 * @param {object} env 原始环境变量
 * @returns {string[]}
 */
export function validateConfig(config, env = {}) {
  const errors = [];

  const numericKeys = [
    'EASYTIER_HEARTBEAT_INTERVAL',
    'EASYTIER_CONNECTION_TIMEOUT',
    'EASYTIER_HANDSHAKE_TIMEOUT',
    'EASYTIER_MAX_MESSAGE_BYTES',
    'EASYTIER_MAX_PEERS_PER_GROUP',
    'EASYTIER_MAX_BYTES_PER_SEC',
    'EASYTIER_MAX_OUT_BYTES_PER_SEC',
    'EASYTIER_MAX_OUT_FRAMES_PER_SEC',
    'EASYTIER_SESSION_TTL_MS',
    'EASYTIER_PEER_CENTER_TTL_MS',
  ];
  for (const key of numericKeys) {
    if (invalidIntEnv(env, key)) errors.push(`${key} 必须是正整数`);
  }

  if (config.connectionTimeoutMs <= config.heartbeatIntervalMs) {
    errors.push('EASYTIER_CONNECTION_TIMEOUT 必须大于 EASYTIER_HEARTBEAT_INTERVAL，否则心跳没发完就会被判定超时');
  }
  if (config.handshakeTimeoutMs < 1000) {
    errors.push('EASYTIER_HANDSHAKE_TIMEOUT 不能小于 1000 毫秒');
  }
  if (config.maxMessageBytes < HEADER_SIZE || config.maxMessageBytes > 16 * 1024 * 1024) {
    errors.push(`EASYTIER_MAX_MESSAGE_BYTES 必须在 ${HEADER_SIZE} 到 16777216 之间`);
  }
  if (config.maxPeersPerGroup < 1 || config.maxPeersPerGroup > 4096) {
    errors.push('EASYTIER_MAX_PEERS_PER_GROUP 必须在 1 到 4096 之间');
  }

  if (config.wsPath && !/^\/[A-Za-z0-9_\-/]*$/.test(config.wsPath)) {
    errors.push('WS_PATH 只能包含字母、数字、下划线、短横线和斜杠');
  }

  // 密钥摘要依赖网络名参与计算，缺少网络名时无法校验，也无从比对
  if (config.networkSecret && !config.networkName) {
    errors.push('配置 EASYTIER_NETWORK_SECRET 时必须同时配置 EASYTIER_NETWORK_NAME');
  }

  const hint = typeof env.LOCATION_HINT === 'string' ? env.LOCATION_HINT.trim() : '';
  if (hint && !LOCATION_HINTS.has(hint)) {
    errors.push(`LOCATION_HINT 不是合法的地域码：${hint}（可选 ${[...LOCATION_HINTS].join('/')}）`);
  }

  if (!env.RELAY_ROOM || typeof env.RELAY_ROOM.idFromName !== 'function') {
    errors.push('缺少 RELAY_ROOM Durable Object 绑定');
  }

  return errors;
}
