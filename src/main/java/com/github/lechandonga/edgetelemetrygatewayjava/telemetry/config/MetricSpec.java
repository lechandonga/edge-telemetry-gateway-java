package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config;

/**
 * 指标规格。hardMin/hardMax 为硬量程（越界拒收）；
 * softMin/softMax 为可信区间（越界降级但仍保留）。
 */
public record MetricSpec(
        String name,
        String unit,
        double hardMin,
        double hardMax,
        double softMin,
        double softMax
) {
    public boolean outsideHardRange(double v) {
        return Double.isNaN(v) || v < hardMin || v > hardMax;
    }

    public boolean outsideSoftRange(double v) {
        return v < softMin || v > softMax;
    }
}
