package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.reconcile;

/**
 * 以“已被网关接收入链的数据”为基准的去向对账。
 *
 * 恒等式（四类互斥且穷尽）：
 * <pre>
 * acceptedIngress == cloudAccepted + quarantined + inFlight + evicted
 * </pre>
 *
 * - acceptedIngress：归档中按幂等键去重后的入链总数（基准）；
 * - cloudAccepted ：云端已确认接收（云端去重计数）；
 * - quarantined   ：被云端永久拒收、已移入隔离区；
 * - inFlight       ：仍在补传缓冲中（含暂时发不出去、等待重试的）；
 * - evicted        ：缓冲写满按 OLDEST_FIRST 淘汰（已淘汰）。
 */
public record ReconciliationReport(
        long acceptedIngress,
        long cloudAccepted,
        long quarantined,
        long inFlight,
        long evicted
) {
    /** 四类去向之和。 */
    public long accounted() {
        return cloudAccepted + quarantined + inFlight + evicted;
    }

    /** 基准与四类去向之和是否精确相等。 */
    public boolean balanced() {
        return acceptedIngress == accounted();
    }
}
