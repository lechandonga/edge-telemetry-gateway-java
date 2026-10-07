package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEvent;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.buffer.PendingBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.Normalizer;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.IngestResult;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.DeadLetterLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.EvictionLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.AcceptedLedger;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.QuarantineLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.TelemetryArchive;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.reconcile.ReconciliationReport;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.CloudEndpoint;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.UplinkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 网关门面：归一化 → 归档 → 告警判定 → 入缓冲补传，串起全链路。
 * 数据先本地持久化（归档 + 缓冲 WAL）再尝试上联，因此任何时刻宕机都不丢已接收数据。
 */
public class TelemetryGateway {

    private static final Logger log = LoggerFactory.getLogger(TelemetryGateway.class);

    private final Normalizer normalizer;
    private final TelemetryArchive archive;
    private final AlertEngine alertEngine;
    private final UplinkManager uplink;
    private final PendingBuffer buffer;
    private final EvictionLog evictionLog;
    private final DeadLetterLog deadLetterLog;
    private final QuarantineLog quarantineLog;
    private final AcceptedLedger acceptedLedger;
    private final CloudEndpoint cloud;

    public TelemetryGateway(Path dataDir, int bufferCapacity, DeviceRegistry registry,
                            CloudEndpoint cloud, AlertEngine alertEngine) {
        this(dataDir, bufferCapacity, registry, cloud, alertEngine, 1);
    }

    /**
     * @param batchSize 补传批大小；&lt;=0 取 {@link UplinkManager#DEFAULT_BATCH_SIZE}，
     *                  超过 {@link UplinkManager#MAX_BATCH_SIZE} 截断到硬上限。
     *                  传 1 即原有逐条补传行为。
     */
    public TelemetryGateway(Path dataDir, int bufferCapacity, DeviceRegistry registry,
                            CloudEndpoint cloud, AlertEngine alertEngine, int batchSize) {
        this.cloud = cloud;
        this.deadLetterLog = new DeadLetterLog(dataDir);
        this.evictionLog = new EvictionLog(dataDir);
        this.quarantineLog = new QuarantineLog(dataDir);
        this.acceptedLedger = new AcceptedLedger(dataDir);
        this.archive = new TelemetryArchive(dataDir);
        this.normalizer = new Normalizer(registry, deadLetterLog);
        this.buffer = new PendingBuffer(dataDir, bufferCapacity, evictionLog, quarantineLog,
                acceptedLedger);
        this.uplink = new UplinkManager(buffer, cloud, batchSize);
        this.alertEngine = alertEngine;
        for (TelemetryRecord record : archive.all()) {
            normalizer.restoreWatermark(record.deviceId(), record.metric(), record.timestamp());
        }
    }

    public IngestResult ingestJson(String rawJson) {
        IngestResult result = normalizer.ingestJson(rawJson);
        afterAccept(result);
        return result;
    }

    public IngestResult ingestRegister(RegisterFrame frame) {
        IngestResult result = normalizer.ingestRegister(frame);
        afterAccept(result);
        return result;
    }

    private void afterAccept(IngestResult result) {
        if (!result.accepted()) {
            return;
        }
        TelemetryRecord record = result.record();
        archive.append(record);
        List<AlertEvent> events = alertEngine.process(record);
        for (AlertEvent event : events) {
            log.info("[GATEWAY] 告警结论 {} rule={} device={} metric={} version={}",
                    event.type(), event.ruleId(), event.deviceId(), event.metric(), event.ruleVersion());
        }
        uplink.submit(record);
    }

    /** 链路恢复后触发补传。 */
    public int flushPending() {
        return uplink.drain();
    }

    public int pendingCount() {
        return uplink.pendingCount();
    }

    public PendingBuffer buffer() {
        return buffer;
    }

    public EvictionLog evictionLog() {
        return evictionLog;
    }

    public DeadLetterLog deadLetterLog() {
        return deadLetterLog;
    }

    /** 永久拒收隔离区（quarantine.jsonl，可查、可按原因分类、跨重启保留）。 */
    public QuarantineLog quarantineLog() {
        return quarantineLog;
    }

    /** 云端已确认接收的本地账本（accepted-keys.jsonl，跨重启保留）。 */
    public AcceptedLedger acceptedLedger() {
        return acceptedLedger;
    }

    public int uplinkBatchSize() {
        return uplink.batchSize();
    }

    public int cloudBatchCallCount() {
        return uplink.cloudBatchCalls();
    }

    /**
     * 以入链归档为基准做四类去向对账。建议在静默点（无并发上报/补传）调用以读取精确快照；
     * 实现上各组件均返回快照，恒等式 acceptedIngress = 云端已接收 + 已隔离 + 在途 + 已淘汰
     * 在静止状态下精确成立。
     */
    public synchronized ReconciliationReport reconcile() {
        Set<String> ingressKeys = new HashSet<>();
        for (TelemetryRecord record : archive.all()) {
            ingressKeys.add(record.idempotencyKey());
        }
        Set<String> evictedKeys = new HashSet<>();
        for (var entry : evictionLog.all()) {
            evictedKeys.add(entry.record().idempotencyKey());
        }
        Set<String> quarantinedKeys = new HashSet<>();
        for (var entry : quarantineLog.all()) {
            quarantinedKeys.add(entry.idempotencyKey());
        }
        Set<String> pendingKeys = new HashSet<>();
        for (TelemetryRecord record : buffer.snapshot()) {
            pendingKeys.add(record.idempotencyKey());
        }
        long inFlight = pendingKeys.stream().filter(ingressKeys::contains).count();
        long quarantined = quarantinedKeys.stream().filter(ingressKeys::contains).count();
        long evicted = evictedKeys.stream().filter(ingressKeys::contains).count();
        // 云端已接收：以入链基准中确实不在其余三类、且云端确认已收下的键计数。
        long cloudAccepted = ingressKeys.stream().filter(acceptedLedger::containsKey)
                .filter(k -> !quarantinedKeys.contains(k) && !evictedKeys.contains(k))
                .count();
        long ingress = ingressKeys.size();
        ReconciliationReport report = new ReconciliationReport(
                ingress, cloudAccepted, quarantined, inFlight, evicted);
        log.info("[RECONCILE] 入链={} 云端已接收={} 已隔离={} 在途={} 已淘汰={} 平衡={}",
                ingress, cloudAccepted, quarantined, inFlight, evicted, report.balanced());
        return report;
    }

    public TelemetryArchive archive() {
        return archive;
    }

    public AlertEngine alertEngine() {
        return alertEngine;
    }
}
