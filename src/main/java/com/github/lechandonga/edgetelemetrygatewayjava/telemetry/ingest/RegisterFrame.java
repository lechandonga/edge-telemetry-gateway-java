package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest;

/**
 * 寄存器采样上报形式（本地模拟 Modbus 风格的一帧读数）。
 *
 * @param deviceId 设备编号
 * @param address  寄存器地址（如 R01），由设备规格映射为指标名
 * @param rawValue 采样原始值（字符串形式，允许解析失败被分类拒收）
 * @param timestamp 采样时间 epoch millis
 */
public record RegisterFrame(String deviceId, String address, String rawValue, Long timestamp) {

    public static final String SOURCE = "REGISTER";
}
