package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

import java.util.Optional;

/**
 * 云端上联端点（本地模拟，无外部服务依赖）。
 */
public interface CloudEndpoint {

    /**
     * 云端接收一条数据。
     *
     * @return 成功返回云端幂等回执；上联不可用返回 empty（网关转入断连缓冲）。
     */
    Optional<String> receive(TelemetryRecord record);

    /**
     * 云端批量接收一批数据（按缓冲 FIFO 顺序）。
     *
     * 默认实现退化为逐条调用 {@link #receive}：每个成功回执为 ACCEPTED，
     * 第一个未获确认（empty）的位置为 UNRESOLVED 并停止——保持原有单条语义。
     * 支持批量的实现可整批一次返回，也可以只返回一个前缀的结果
     * （见 {@link BatchReceipt} 的前缀解释规则）。
     *
     * @return 整批回执；上联不可用/整批回执丢失时返回 empty（全部留队重发）。
     */
    default java.util.Optional<BatchReceipt> receiveBatch(java.util.List<TelemetryRecord> batch) {
        if (!isAvailable()) {
            return Optional.empty();
        }
        java.util.List<ItemOutcome> outcomes = new java.util.ArrayList<>(batch.size());
        for (TelemetryRecord record : batch) {
            Optional<String> ack = receive(record);
            if (ack.isEmpty()) {
                outcomes.add(ItemOutcome.unresolved());
                break;
            }
            outcomes.add(ItemOutcome.accepted(ack.get()));
        }
        return Optional.of(new BatchReceipt(outcomes));
    }

    /** 云端是否可达（模拟断连/恢复）。 */
    boolean isAvailable();

    /** 云端已去重接收的数据条数（用于测试断言“不重”）。 */
    int acceptedCount();
}
