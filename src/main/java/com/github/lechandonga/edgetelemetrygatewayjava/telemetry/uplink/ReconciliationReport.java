package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

/**
 * 对账报告：以“被网关接收入链的数据”为基准（归档总数），云端已接收、
 * 已隔离、仍在途、已淘汰四类数字分别可查，且必须精确配平：
 *
 * <pre>
 * acceptedIntoChain = cloudAccepted + quarantined + inFlight + evicted
 * </pre>
 *
 * 口径：
 * <ul>
 *   <li>{@code acceptedIntoChain}：归一化通过并写入归档的数据总数；</li>
 *   <li>{@code inFlight}：仍在补传队列、尚未拿到最终结论的数据数；</li>
 *   <li>{@code quarantined}：云端永久拒收、已转入隔离区的数据数；</li>
 *   <li>{@code evicted}：缓冲写满按 OLDEST_FIRST 淘汰、记入淘汰日志的数据数；</li>
 *   <li>{@code cloudAccepted}：按配平式计算的“云端最终接收”数
 *       （已确认出队且未隔离、未淘汰）；与云端 {@code acceptedCount()} 互相印证。</li>
 * </ul>
 */
public record ReconciliationReport(
        long acceptedIntoChain,
        long cloudAccepted,
        long quarantined,
        long inFlight,
        long evicted
) {
    public boolean balanced() {
        return acceptedIntoChain == cloudAccepted + quarantined + inFlight + evicted;
    }

    @Override
    public String toString() {
        return String.format(
                "对账: 入链=%d 云端已接收=%d 已隔离=%d 仍在途=%d 已淘汰=%d 配平=%s",
                acceptedIntoChain, cloudAccepted, quarantined, inFlight, evicted, balanced());
    }
}
