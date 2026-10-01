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
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.TelemetryArchive;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.CloudEndpoint;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.UplinkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

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

    public TelemetryGateway(Path dataDir, int bufferCapacity, DeviceRegistry registry,
                            CloudEndpoint cloud, AlertEngine alertEngine) {
        this.deadLetterLog = new DeadLetterLog(dataDir);
        this.evictionLog = new EvictionLog(dataDir);
        this.archive = new TelemetryArchive(dataDir);
        this.normalizer = new Normalizer(registry, deadLetterLog);
        this.buffer = new PendingBuffer(dataDir, bufferCapacity, evictionLog);
        this.uplink = new UplinkManager(buffer, cloud);
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

    public TelemetryArchive archive() {
        return archive;
    }

    public AlertEngine alertEngine() {
        return alertEngine;
    }
}
