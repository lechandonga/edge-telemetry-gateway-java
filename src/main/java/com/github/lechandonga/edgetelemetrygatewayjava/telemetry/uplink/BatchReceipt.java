package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import java.util.List;

/**
 * 云端对一个补传批次的回执。
 *
 * 关键语义（云端必须遵守，网关也只按此处理）：
 * - 回执只允许覆盖“批次的一个顺序前缀”：可以整批确认，也可以只回前面若干条；
 * - 前缀内逐条给出结论；前一个下标未确认（RETRYABLE/缺回执）时，
 *   其后的任何结论网关一律不处理——未确认的一条都不能被后面数据插队；
 * - 整批回执丢失用 {@link #noneLost()} 的反面表达：
 *   {@code BatchReceipt.lost()} 返回空回执，等价于“一条都没确认”，整批原样重试；
 * - 重复批次（重发）的回执与首次结构一致即可，云端按幂等键去重，重复确认天然安全。
 */
public record BatchReceipt(List<RecordReceipt> records) {

    public BatchReceipt {
        records = List.copyOf(records);
    }

    /** 整批回执丢失：没有任何记录得到确认，整批必须留在缓冲重试。 */
    public static BatchReceipt lost() {
        return new BatchReceipt(List.of());
    }

    public static BatchReceipt of(List<RecordReceipt> records) {
        return new BatchReceipt(records);
    }

    public int size() {
        return records.size();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }
}
