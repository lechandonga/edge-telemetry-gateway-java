package com.github.lechandonga.edgetelemetrygatewayjava.ingest;

import java.util.Map;

/**
 * 已登记设备的规格：寄存器地址 -> 指标名，指标 -> [min,max] 合法区间。
 */
public record DeviceSpec(
        String deviceId,
        Map<Integer, String> registerMap,
        Map<String, double[]> metricRanges
) {
}
