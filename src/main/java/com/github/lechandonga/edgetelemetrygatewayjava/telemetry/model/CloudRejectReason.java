package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model;

/**
 * 云端对单条遥测数据给出的“不可恢复拒收”原因分类。
 *
 * 与 {@link RejectReason}（网关入口校验拒收）区分：这里的记录已经被网关
 * 正常接收入链，只是云端明确表示永远不会接收，继续重试没有意义。
 * 命中后记录转入隔离区，绝不在补传队头无限重试，也绝不静默丢弃。
 */
public enum CloudRejectReason {
    /** 历史数据超过云端接收时限（如只接收近 N 天数据）。 */
    EXPIRED,
    /** 云端已不再接收该指标（指标下线/被替换）。 */
    METRIC_DISCONTINUED,
    /** 其他云端明确给出的不可恢复拒收原因。 */
    PERMANENTLY_REJECTED
}
