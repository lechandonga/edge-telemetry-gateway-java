package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model;

/**
 * 可区分的拒收原因分类。拒收记录进入死信日志，绝不静默丢弃。
 */
public enum RejectReason {
    /** 报文无法解析（不是合法 JSON / 寄存器帧字段损坏） */
    MALFORMED,
    /** 设备未在网关注册 */
    UNKNOWN_DEVICE,
    /** 指标未在设备规格中声明 */
    UNKNOWN_METRIC,
    /** 必填字段缺失（设备号/指标/值/时间戳等） */
    MISSING_FIELD,
    /** 数值无法转为 double，或为 NaN/Infinity */
    INVALID_VALUE,
    /** 数值超出指标硬量程 [hardMin, hardMax] */
    OUT_OF_RANGE,
    /** 时间戳早于该设备已接受的最大时间戳（每设备顺序保证） */
    OUT_OF_ORDER,
    /** 幂等键重复：同设备同指标同时间戳已接收 */
    DUPLICATE
}
