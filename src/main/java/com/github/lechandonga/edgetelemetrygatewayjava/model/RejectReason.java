package com.github.lechandonga.edgetelemetrygatewayjava.model;

/** 可区分的拒收/降级原因分类。 */
public enum RejectReason {
    /** 字段缺失（设备、指标、值、时间戳任一为空）。 */
    MISSING_FIELD,
    /** 数值越界或无法解析。 */
    VALUE_OUT_OF_RANGE,
    /** 来源设备未登记。 */
    UNKNOWN_DEVICE,
    /** 原始报文无法解析。 */
    MALFORMED_PAYLOAD,
    /** 时间戳乱序（作为拒收策略时使用；默认策略为钳制降级）。 */
    OUT_OF_ORDER
}
