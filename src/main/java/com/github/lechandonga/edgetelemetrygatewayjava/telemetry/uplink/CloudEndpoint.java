package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

import java.util.ArrayList;
import java.util.List;
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
     * 云端按批接收一批数据（批内顺序即缓冲 FIFO 顺序，且不会跨设备重排）。
     *
     * 默认实现退化为逐条接收，因此既有云端实现与单条补传行为完全不受影响。
     * 真实/模拟实现可整批返回一次回执；回执只允许覆盖批次的顺序前缀，
     * 详见 {@link BatchReceipt}。
     */
    default BatchReceipt receiveBatch(List<TelemetryRecord> batch) {
        List<RecordReceipt> receipts = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            TelemetryRecord record = batch.get(i);
            Optional<String> ack = receive(record);
            if (ack.isEmpty()) {
                // 顺序前缀语义：第一条未确认即停止，后面的一律不带结论。
                break;
            }
            receipts.add(RecordReceipt.accepted(i, record.idempotencyKey()));
        }
        return new BatchReceipt(receipts);
    }

    /** 云端批量接收接口被调用的次数（衡量补传效率：应与批次数同量级）。 */
    default int batchCallCount() {
        return 0;
    }

    /** 云端是否已去重接收指定幂等键（对账用；默认不可判定，模拟端提供精确实现）。 */
    default boolean hasReceived(String key) {
        return false;
    }

    /** 云端是否可达（模拟断连/恢复）。 */
    boolean isAvailable();

    /** 云端已去重接收的数据条数（用于测试断言“不重”）。 */
    int acceptedCount();
}
