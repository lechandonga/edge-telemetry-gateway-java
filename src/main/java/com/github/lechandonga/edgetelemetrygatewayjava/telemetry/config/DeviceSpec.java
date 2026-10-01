package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config;

import java.util.Map;

/**
 * 设备登记规格：设备编号、名称与其支持的指标集合。
 */
public record DeviceSpec(String deviceId, String name, Map<String, MetricSpec> metrics,
                         Map<String, String> registerAliases) {
    public MetricSpec metric(String metric) {
        return metrics.get(metric);
    }

    /** 寄存器地址 -> 指标名。 */
    public String resolveRegister(String address) {
        return registerAliases.get(address);
    }
}
