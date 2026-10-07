package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model;

/**
 * 云端对一条已入链数据的回执分类。
 *
 * 与 {@link RejectReason} 的区别：后者是入口归一化阶段拒收（数据从未入链），
 * 本枚举是数据已被网关接收并尝试上联后，云端对该记录给出的处理结论。
 */
public enum CloudRejectCode {
    /**
     * 可恢复失败：链路抖动、云端暂时不可用、超时等。
     * 记录继续留在缓冲队头，后续原样重试——“暂时发不出去”。
     */
    RETRYABLE,
    /** 历史数据超过云端接收时限，永久不再接收。 */
    EXPIRED,
    /** 云端已下线/不再接收该指标，永久拒收。 */
    METRIC_DISCONTINUED
}
