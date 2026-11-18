import { HEADER_SIZE } from './constants.js';

/**
 * 解析 16 字节的 EasyTier 包头。
 *
 * 使用 DataView 直接读写，避免为每个报文创建 Buffer 视图产生额外分配。
 */
export function parseHeader(buffer) {
  if (!buffer || buffer.length < HEADER_SIZE) return null;
  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
  return {
    fromPeerId: view.getUint32(0, true),
    toPeerId: view.getUint32(4, true),
    packetType: view.getUint8(8),
    flags: view.getUint8(9),
    forwardCounter: view.getUint8(10),
    reserved: view.getUint8(11),
    len: view.getUint32(12, true),
  };
}

export function createHeader(fromPeerId, toPeerId, packetType, payloadLen, flags = 0) {
  const buffer = new Uint8Array(HEADER_SIZE);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, fromPeerId >>> 0, true);
  view.setUint32(4, toPeerId >>> 0, true);
  view.setUint8(8, packetType);
  view.setUint8(9, flags);
  view.setUint8(10, 1); // forwardCounter
  view.setUint8(11, 0);
  view.setUint32(12, payloadLen >>> 0, true);
  return buffer;
}
