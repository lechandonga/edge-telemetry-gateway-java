package com.github.lechandonga.edgetelemetrygatewayjava.uplink;

/** 单次补传尝试结果。 */
public enum UplinkResult {
    /** 云端首次接收并确认。 */
    DELIVERED,
    /** 云端已有该 recordId，重复接收仍返回确认（幂等）。 */
    DELIVERED_DUPLICATE,
    /** 链路不可用，保留在缓冲。 */
    LINK_DOWN
}
