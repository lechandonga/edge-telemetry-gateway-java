package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import java.util.ArrayList;
import java.util.List;

/**
 * 云端对一个补传批次的回执。
 *
 * 云端可以：
 * <ul>
 *   <li>整批回一次结果：{@code outcomes} 与请求批次等长；</li>
 *   <li>只回前面一部分（只确认前 k 条）：{@code outcomes} 长度小于批大小；</li>
 *   <li>整批回执丢失/调用失败：网关得到 empty（{@link UplinkManager} 全部留队重发）。</li>
 * </ul>
 *
 * 网关按批内顺序解释结果（前缀语义，保证不重不漏、不插队）：
 * <ol>
 *   <li>从批首逐条处理：ACCEPTED → 确认出队；PERMANENTLY_REJECTED → 隔离后出队；</li>
 *   <li>遇到第一条 UNRESOLVED 立即停止，该条及其后所有数据（含未给结果的部分）
 *       一律保留在补传队列，下一批次重发；</li>
 *   <li>缺失结果（outcomes 比批次短）等价于尾部 UNRESOLVED。</li>
 * </ol>
 * 因此云端只可能安全地“确认一个前缀”，任何没确认的数据都不会丢、
 * 不会被后面的数据插队，重发由云端幂等去重保证不重。
 */
public record BatchReceipt(List<ItemOutcome> outcomes) {

    public BatchReceipt {
        outcomes = List.copyOf(outcomes);
    }

    public static BatchReceipt of(ItemOutcome... outcomes) {
        return new BatchReceipt(List.of(outcomes));
    }

    /** 全部确认接收（无永久拒收、无未决）。 */
    public static BatchReceipt allAccepted(int count) {
        List<ItemOutcome> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(ItemOutcome.accepted(null));
        }
        return new BatchReceipt(list);
    }
}
