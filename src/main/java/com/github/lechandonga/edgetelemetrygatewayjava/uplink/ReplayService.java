package com.github.lechandonga.edgetelemetrygatewayjava.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.buffer.AckOutcome;
import com.github.lechandonga.edgetelemetrygatewayjava.buffer.TelemetryBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

import java.util.List;
import java.util.function.Consumer;

/**
 * 补传服务：恢复后按 recordId 升序逐条补传并确认。
 *
 * 幂等保证：
 * - 云端按 recordId 去重，重复上报得到相同确认；
 * - 缓冲 confirm 幂等，进程重启后再次补传不会产生重复；
 * - 每条记录采用“先云端落库、后本地确认”顺序；崩溃在二者之间时，
 *   重启后重发同一条，云端去重，仍然恰好一次（有效）。
 */
public class ReplayService {

    private final TelemetryBuffer buffer;
    private final UplinkLink link;
    private final CloudIngestService cloud;
    private final Consumer<String> logger;

    public ReplayService(TelemetryBuffer buffer, UplinkLink link, CloudIngestService cloud,
                         Consumer<String> logger) {
        this.buffer = buffer;
        this.link = link;
        this.cloud = cloud;
        this.logger = logger != null ? logger : s -> {};
    }

    /** 处理单条：链路通时上报并确认；断时保留缓冲。 */
    public UplinkResult deliverOne(TelemetryRecord record) {
        if (!link.isAvailable()) {
            return UplinkResult.LINK_DOWN;
        }
        boolean firstTime = cloud.ingest(record);
        AckOutcome ack = buffer.confirm(record.recordId());
        logger.accept(String.format(
                "[uplink] recordId=%d device=%s cloudFirst=%s ack=%s",
                record.recordId(), record.deviceId(), firstTime, ack));
        return firstTime ? UplinkResult.DELIVERED : UplinkResult.DELIVERED_DUPLICATE;
    }

    /**
     * 按序补传全部 pending；遇到断链立即停止（后续仍有序保留）。
     * @return 本次确认数。
     */
    public int replayPending() {
        List<TelemetryRecord> pending = buffer.pendingRecords();
        int delivered = 0;
        for (TelemetryRecord r : pending) {
            UplinkResult result = deliverOne(r);
            if (result == UplinkResult.LINK_DOWN) {
                logger.accept("[uplink] link down during replay, remaining="
                        + buffer.pendingRecords().size());
                break;
            }
            delivered++;
        }
        return delivered;
    }
}
