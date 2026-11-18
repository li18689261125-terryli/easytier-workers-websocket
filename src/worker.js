// Cloudflare Worker 入口：把 WebSocket 请求路由到 Durable Object
import { RelayRoom } from './worker/relay_room.js';
import { loadConfig, matchesWsPath, validateConfig } from './worker/core/config.js';

export { RelayRoom };

/** 把 room 参数收敛到安全字符集，避免用畸形值映射到任意 Durable Object 实例名 */
function normalizeRoom(raw) {
  return /^[A-Za-z0-9_-]{1,64}$/.test(raw) ? raw : 'default';
}

/** 解析 Authorization: Bearer <token> */
function bearerToken(request) {
  const h = request.headers.get('Authorization') || '';
  const m = /^Bearer\s+(.+)$/i.exec(h);
  return m ? m[1].trim() : '';
}

/** 常量时间比较令牌，避免通过响应耗时逐字符猜测 */
async function tokenMatches(provided, expected) {
  const enc = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest('SHA-256', enc.encode(provided)),
    crypto.subtle.digest('SHA-256', enc.encode(expected)),
  ]);
  const x = new Uint8Array(a);
  const y = new Uint8Array(b);
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
  return diff === 0;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const config = loadConfig(env);

    if (url.pathname === '/healthz') {
      // 配置自检：写错的环境变量在启动阶段就暴露为 503，避免线上只看到 ok 却连不上
      const errors = validateConfig(config, env);
      if (errors.length > 0) {
        return new Response(JSON.stringify({ ok: false, errors }), {
          status: 503,
          headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Cache-Control': 'no-store',
          },
        });
      }
      return new Response('ok', { status: 200, headers: { 'Cache-Control': 'no-store' } });
    }

    // 负载统计：未配置 EASYTIER_STATS_TOKEN 时视为未启用，直接 404（不泄露该接口存在）
    if (url.pathname === '/stats') {
      const expected = typeof env.EASYTIER_STATS_TOKEN === 'string' ? env.EASYTIER_STATS_TOKEN.trim() : '';
      if (!expected) {
        return new Response('Not found', { status: 404 });
      }
      if (request.method !== 'GET') {
        return new Response('Method not allowed', { status: 405, headers: { Allow: 'GET' } });
      }
      const provided = bearerToken(request);
      if (!provided || !(await tokenMatches(provided, expected))) {
        return new Response('Forbidden', { status: 403 });
      }

      const roomId = normalizeRoom(url.searchParams.get('room') || 'default');
      const options = env.LOCATION_HINT ? { locationHint: env.LOCATION_HINT } : {};
      const roomStub = env.RELAY_ROOM.get(env.RELAY_ROOM.idFromName(roomId), options);

      // 用独立的内部路径转发给 DO，避免与 WebSocket 入口混淆
      const statsUrl = new URL(request.url);
      statsUrl.pathname = '/__stats';
      statsUrl.search = `?room=${encodeURIComponent(roomId)}`;
      const resp = await roomStub.fetch(new Request(statsUrl.toString(), { method: 'GET' }));
      return new Response(resp.body, {
        status: resp.status,
        headers: {
          'Content-Type': resp.headers.get('Content-Type') || 'application/json; charset=utf-8',
          'Cache-Control': 'no-store',
        },
      });
    }

    if (!matchesWsPath(config, url.pathname)) {
      return new Response('Not found', { status: 404 });
    }

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 400 });
    }

    const roomId = normalizeRoom(url.searchParams.get('room') || 'default');
    const options = env.LOCATION_HINT ? { locationHint: env.LOCATION_HINT } : {};
    const roomStub = env.RELAY_ROOM.get(env.RELAY_ROOM.idFromName(roomId), options);
    return roomStub.fetch(request);
  },
};
