package com.github.lechandonga.edgetelemetrygatewayjava.model;

/**
 * 数据质量。降级记录（如时间戳乱序被修正）仍参与链路，但标记质量。
 */
public enum Quality {
    /** 正常。 */
    GOOD,
    /** 乱序时间戳已被钳制为设备最新时间，数据保留但降级。 */
    TIMESTAMP_CLAMPED
}
