export const MAGIC = 0xd1e1a5e1;
export const VERSION = 1;
export const MY_PEER_ID = 10000001; // Server Peer ID
export const HEADER_SIZE = 16;

export const PacketType = {
  Invalid: 0,
  Data: 1,
  HandShake: 2,
  RoutePacket: 3, // deprecated
  Ping: 4,
  Pong: 5,
  TaRpc: 6, // deprecated
  Route: 7, // deprecated
  RpcReq: 8,
  RpcResp: 9,
  ForeignNetworkPacket: 10,
  KcpSrc: 11,
  KcpDst: 12,
};

/**
 * 应用自定义关闭码（4400-4999 段），便于客户端与日志区分断线原因。
 */
export const CloseCode = {
  Unauthorized: 4401, // 握手非法：magic 不符、peerId 非法、密钥摘要非法
  NetworkRejected: 4403, // 网络名不匹配（私有模式）或网络未配置
  HandshakeTimeout: 4408, // 超过握手超时仍未完成握手
};
