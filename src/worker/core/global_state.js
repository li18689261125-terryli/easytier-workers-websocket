/**
 * PeerCenter 全局状态
 *
 * 原实现使用模块级 Map，会在同一 isolate 内的多个 Durable Object 实例之间
 * 共享（串台）且没有上限。现在改为实例化，由 RelayRoom 持有。
 *
 * @file global_state.js
 */

import { MY_PEER_ID } from './constants.js';

export class GlobalState {
  constructor(config) {
    this.peerCenterStateByGroup = new Map();
    this.peerCenterTtlMs = config.peerCenterTtlMs;
    this.cleanInterval = Math.max(30_000, Math.min(this.peerCenterTtlMs / 2, 120_000));
    this.lastPeerCenterClean = 0;
  }

  getPeerCenterState(groupKey) {
    const k = String(groupKey || '');
    let state = this.peerCenterStateByGroup.get(k);
    if (!state) {
      state = { globalPeerMap: new Map(), digest: '0' };
      this.peerCenterStateByGroup.set(k, state);
    }
    const now = Date.now();
    if (now - this.lastPeerCenterClean > this.cleanInterval) {
      this.clean(now);
    }
    state.lastTouch = now;
    return state;
  }

  clean(now = Date.now()) {
    this.lastPeerCenterClean = now;
    for (const [groupKey, state] of this.peerCenterStateByGroup.entries()) {
      for (const [peerId, info] of state.globalPeerMap.entries()) {
        if (now - (info.lastSeen || 0) > this.peerCenterTtlMs) {
          state.globalPeerMap.delete(peerId);
        }
      }
      if (now - (state.lastTouch || 0) > this.peerCenterTtlMs && state.globalPeerMap.size === 0) {
        this.peerCenterStateByGroup.delete(groupKey);
      }
    }
  }

  /** 清理某个 peer 本身，以及它在其它 peer 下登记的子设备 */
  cleanPeerAndSubPeers(groupKey, peerId) {
    const state = this.getPeerCenterState(groupKey);
    const peerIdStr = String(peerId);

    state.globalPeerMap.delete(peerIdStr);

    for (const [, peerInfo] of state.globalPeerMap.entries()) {
      if (peerInfo.directPeers && peerInfo.directPeers[peerIdStr]) {
        delete peerInfo.directPeers[peerIdStr];
      }
    }
  }
}

/**
 * 计算 peer 中心快照的摘要，用于让客户端判断是否需要重新拉取。
 * 使用 64 位滚动哈希，同步计算。
 */
export function calcPeerCenterDigest(snapshot) {
  const str = JSON.stringify(snapshot);
  let hash = 0n;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5n) - hash) + BigInt(str.charCodeAt(i));
    hash &= 0xFFFFFFFFFFFFFFFFn;
  }
  return hash.toString();
}

/** 构建下发给客户端的全局 peer 映射 */
export function buildPeerCenterResponseMap(groupKey, state, peerManager) {
  const out = {};
  const allKnownPeers = new Set();
  const directPeers = peerManager.listPeerIdsInGroup(groupKey);
  const directPeerSet = new Set(directPeers);

  for (const peerId of directPeers) allKnownPeers.add(peerId);

  const infos = peerManager._getPeerInfosMap(groupKey, false);
  if (infos) {
    for (const pid of infos.keys()) allKnownPeers.add(pid);
  }

  for (const [peerId, peerInfo] of state.globalPeerMap.entries()) {
    allKnownPeers.add(Number(peerId));
    if (peerInfo.directPeers) {
      for (const subPeerId of Object.keys(peerInfo.directPeers)) {
        allKnownPeers.add(Number(subPeerId));
      }
    }
  }

  for (const peerId of allKnownPeers) {
    const key = String(peerId);
    const existing = state.globalPeerMap.get(key);
    out[key] = existing ? { ...existing } : { directPeers: {} };
    if (!out[key].directPeers) out[key].directPeers = {};

    if (directPeerSet.has(peerId)) {
      out[key].directPeers[String(MY_PEER_ID)] = { latencyMs: 0 };
    }

    if (existing && existing.directPeers) {
      for (const [subPeerId, subInfo] of Object.entries(existing.directPeers)) {
        out[key].directPeers[subPeerId] = { ...subInfo };
      }
    }
  }

  return out;
}
