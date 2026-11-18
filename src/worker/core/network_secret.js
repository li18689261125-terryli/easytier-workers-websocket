/**
 * 网络密钥摘要（与 EasyTier 客户端保持一致的算法）
 *
 * EasyTier 的 HandshakeRequest.network_secret_digest 并非标准哈希，而是：
 *   hasher = Rust DefaultHasher（SipHash-1-3，key 固定为 0）
 *   hasher.write(network_name); hasher.write(network_secret)
 *   对 32 字节摘要分 4 片，每片写 finish().to_be_bytes()，再把已生成的摘要前缀喂回 hasher
 *
 * 因此这里必须手写 SipHash-1-3 才能与服务端配置的密钥算出同一个摘要。
 * 注意必须与 Rust core 的 sip.rs 完全一致：压缩 1 轮、终结 3 轮，终结时 v2 ^= 0xff。
 *
 * @file network_secret.js
 */

const MASK64 = (1n << 64n) - 1n;
const K0 = 0x736f6d6570736575n;
const K1 = 0x646f72616e646f6dn;
const K2 = 0x6c7967656e657261n;
const K3 = 0x7465646279746573n;

function rotl(x, n) {
  const s = BigInt(n);
  return ((x << s) | (x >> (64n - s))) & MASK64;
}

/** 一轮 SipRound */
function sipRound(v) {
  v.v0 = (v.v0 + v.v1) & MASK64;
  v.v1 = rotl(v.v1, 13) ^ v.v0;
  v.v0 = rotl(v.v0, 32);

  v.v2 = (v.v2 + v.v3) & MASK64;
  v.v3 = rotl(v.v3, 16) ^ v.v2;

  v.v0 = (v.v0 + v.v3) & MASK64;
  v.v3 = rotl(v.v3, 21) ^ v.v0;

  v.v2 = (v.v2 + v.v1) & MASK64;
  v.v1 = rotl(v.v1, 17) ^ v.v2;
  v.v2 = rotl(v.v2, 32);
}

/** SipHash-1-3，key 固定 0（等价 Rust DefaultHasher），支持 write / finish 交替调用 */
class SipHash13 {
  constructor() {
    this.v0 = K0;
    this.v1 = K1;
    this.v2 = K2;
    this.v3 = K3;
    this.tail = 0n;
    this.ntail = 0;
    this.length = 0n;
  }

  write(bytes) {
    const len = bytes.length;
    this.length += BigInt(len);
    let i = 0;

    if (this.ntail !== 0) {
      const needed = 8 - this.ntail;
      const take = Math.min(len, needed);
      for (let k = 0; k < take; k++) {
        this.tail |= BigInt(bytes[k]) << BigInt(8 * (this.ntail + k));
      }
      if (len < needed) {
        this.ntail += len;
        return;
      }
      this.v3 ^= this.tail;
      sipRound(this);
      this.v0 ^= this.tail;
      this.ntail = 0;
      i = needed;
    }

    while (i + 8 <= len) {
      let m = 0n;
      for (let k = 7; k >= 0; k--) m = (m << 8n) | BigInt(bytes[i + k]);
      this.v3 ^= m;
      sipRound(this);
      this.v0 ^= m;
      i += 8;
    }

    this.tail = 0n;
    for (let k = 0; i + k < len; k++) {
      this.tail |= BigInt(bytes[i + k]) << BigInt(8 * k);
    }
    this.ntail = len - i;
  }

  finish() {
    const state = { v0: this.v0, v1: this.v1, v2: this.v2, v3: this.v3 };
    const b = ((this.length & 0xffn) << 56n) | this.tail;

    state.v3 ^= b;
    sipRound(state);
    state.v0 ^= b;

    state.v2 ^= 0xffn;
    sipRound(state);
    sipRound(state);
    sipRound(state);

    return (state.v0 ^ state.v1 ^ state.v2 ^ state.v3) & MASK64;
  }
}

/**
 * 计算 EasyTier 的 network_secret_digest。
 *
 * @param {string} networkName 网络名
 * @param {string} networkSecret 网络密钥
 * @returns {Uint8Array} 32 字节摘要
 */
export function networkSecretDigest(networkName, networkSecret) {
  const encoder = new TextEncoder();
  const hasher = new SipHash13();
  hasher.write(encoder.encode(networkName));
  hasher.write(encoder.encode(networkSecret));

  const digest = new Uint8Array(32);
  for (let i = 0; i < 4; i++) {
    const h = hasher.finish();
    for (let k = 0; k < 8; k++) {
      digest[i * 8 + k] = Number((h >> BigInt(56 - 8 * k)) & 0xffn);
    }
    hasher.write(digest.subarray(0, (i + 1) * 8));
  }
  return digest;
}

/**
 * 常量时间比较两个字节数组是否相等。
 * 长度不同直接返回 false（摘要固定 32 字节，长度本身不是秘密）。
 */
export function timingSafeEqual(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return diff === 0;
}
