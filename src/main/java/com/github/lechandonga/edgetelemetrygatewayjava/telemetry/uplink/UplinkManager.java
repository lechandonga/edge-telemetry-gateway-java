package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * 补传编排：先持久化入缓冲，再尝试上联（write-ahead）。
 *
 * 不丢：数据入队成功即落盘，只有收到云端幂等回执后才出队；
 * 不重：云端按 idempotencyKey 去重，网关出队校验队头 key，重复 ACK 无效；
 * 有序：严格按全局 FIFO 队头发送，前一条未确认则不发后一条；
 * 重启：缓冲从 WAL 恢复，进程重启后 drain() 继续补传。
 */
public class UplinkManager {

    private static final Logger log = LoggerFactory.getLogger(UplinkManager.class);

    private final PendingBuffer buffer;
    private final CloudEndpoint cloud;

    public UplinkManager(PendingBuffer buffer, CloudEndpoint cloud) {
        this.buffer = buffer;
        this.cloud = cloud;
    }

    /**
     * 接收一条已归一化数据：落盘入队，并尽力立即发送。
     * @return 是否为缓冲中新记录（false 表示重复幂等键被忽略）
     */
    public boolean submit(TelemetryRecord record) {
        boolean enqueued = buffer.offer(record);
        if (!enqueued) {
            log.info("[UPLINK] 缓冲中已存在 key={}，幂等忽略", record.idempotencyKey());
            return false;
        }
        log.info("[UPLINK] 入缓冲 key={} 缓冲占用={}/{}",
                record.idempotencyKey(), buffer.size(), buffer.capacity());
        drain();
        return true;
    }

    /**
     * 按 FIFO 补传缓冲中所有能发送的数据；遇到断连即停止，已恢复后重发。
     *
     * @return 本次成功确认出队的条数
     */
    public int drain() {
        int confirmed = 0;
        while (true) {
            TelemetryRecord head;
            String key;
            synchronized (buffer) {
                head = buffer.peek();
            }
            if (head == null) {
                break;
            }
            if (!cloud.isAvailable()) {
                log.info("[UPLINK] 上联不可用，暂停补传，滞留 {} 条", buffer.size());
                break;
            }
            key = head.idempotencyKey();
            Optional<String> ack = cloud.receive(head);
            if (ack.isEmpty()) {
                log.warn("[UPLINK] 发送未获确认 key={}（可能断连/ACK丢失），留待重试", key);
                break;
            }
            boolean removed;
            synchronized (buffer) {
                removed = buffer.ack(key);
            }
            if (!removed) {
                log.warn("[UPLINK] 确认 key={} 时队头已变化，忽略该确认", key);
                break;
            }
            confirmed++;
            log.info("[UPLINK] 确认出队 key={} ack={} 剩余={}", key, ack.get(), buffer.size());
        }
        return confirmed;
    }

    public int pendingCount() {
        return buffer.size();
    }
}
