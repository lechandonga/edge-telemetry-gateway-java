package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model;

import java.util.List;

/**
 * 统一遥测模型。寄存器采样与 JSON 报文在入口处归一化为本结构。
 *
 * @param sequence  设备内单调递增序号，用于乱序/重复判定与稳定排序
 * @param deviceId  来源设备
 * @param metric    指标名（如 temperature）
 * @param value     采样值
 * @param timestamp 采样时间（epoch millis）
 * @param source    上报形式：REGISTER / JSON
 * @param quality   质量等级
 * @param degradeReasons 降级原因（quality=DEGRADED 时非空）
 */
public record TelemetryRecord(
        long sequence,
        String deviceId,
        String metric,
        double value,
        long timestamp,
        String source,
        Quality quality,
        List<String> degradeReasons
) {
    /**
     * 幂等键：同一设备、同一指标、同一时间戳的采样视为同一条数据。
     */
    public String idempotencyKey() {
        return deviceId + "|" + metric + "|" + timestamp;
    }

    public TelemetryRecord withDegraded(List<String> reasons) {
        return new TelemetryRecord(sequence, deviceId, metric, value, timestamp, source,
                Quality.DEGRADED, List.copyOf(reasons));
    }
}
