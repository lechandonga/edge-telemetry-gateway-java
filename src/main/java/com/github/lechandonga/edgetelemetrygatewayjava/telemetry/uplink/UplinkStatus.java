package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

/**
 * 补传单条结论三分类：
 *
 * ACCEPTED             —— 云端确认接收，允许出队；
 * RETRYABLE            —— 暂时发不出去（断连/超时/整批回执丢失），留在缓冲按序重试；
 * PERMANENTLY_REJECTED —— 云端明确永久拒收，从缓冲移入隔离区，不占队头、不静默消失。
 */
public enum UplinkStatus {
    ACCEPTED,
    RETRYABLE,
    PERMANENTLY_REJECTED
}
