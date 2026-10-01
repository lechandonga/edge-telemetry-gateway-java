package com.github.lechandonga.edgetelemetrygatewayjava.ingest;

/** 寄存器采样上报形式：地址 + 原始数值 + 采样时间。 */
public record RegisterSample(
        String deviceId,
        int registerAddress,
        Double value,
        Long timestampMs
) {
}
