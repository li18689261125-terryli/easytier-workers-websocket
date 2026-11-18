import zlib from 'zlib';

/**
 * 入站 RPC 报文解压（尽力而为）。
 *
 * 本 Worker 对外只声明支持 algo=1（None），正常不会收到压缩体；
 * 若客户端仍然发送压缩数据，这里尝试按 gzip 解压，失败由调用方丢弃。
 */
export function gunzipMaybe(data) {
  return zlib.gunzipSync(data);
}
