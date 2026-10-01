package com.github.lechandonga.edgetelemetrygatewayjava.pipeline;

import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.buffer.BufferStats;
import com.github.lechandonga.edgetelemetrygatewayjava.buffer.TelemetryBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.RegisterSample;
import com.github.lechandonga.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.CloudIngestService;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.ReplayService;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.UplinkLink;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.UplinkResult;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * 网关编排：
 * 1) 归一化（拒收/降级分类返回，不静默丢弃）；
 * 2) 接受的记录先进本地有界缓冲（再断链也不丢）；
 * 3) 链路通则立即补传确认；断则留在缓冲，恢复后 onLinkRestored 按序补传；
 * 4) 每条进入缓冲的记录同步评估版本化告警。
 */
public class Gateway {

    private final Normalizer normalizer;
    private final TelemetryBuffer buffer;
    private final AlertEngine alertEngine;
    private final ReplayService replayService;
    private final UplinkLink link;
    private final Consumer<String> logger;

    public Gateway(Normalizer normalizer, TelemetryBuffer buffer, AlertEngine alertEngine,
                   UplinkLink link, CloudIngestService cloud, Consumer<String> logger) {
        this.normalizer = normalizer;
        this.buffer = buffer;
        this.alertEngine = alertEngine;
        this.link = link;
        this.logger = logger != null ? logger : s -> {};
        this.replayService = new ReplayService(buffer, link, cloud, this.logger);
    }

    /** 接收寄存器采样。 */
    public IngestResult ingestRegister(RegisterSample sample) {
        IngestResult result = normalizer.normalize(sample);
        logIngest("register", String.valueOf(sample), result);
        handleAccepted(result);
        return result;
    }

    /** 接收 JSON 报文。 */
    public IngestResult ingestJson(String json) {
        IngestResult result = normalizer.normalizeJson(json);
        logIngest("json", json, result);
        handleAccepted(result);
        return result;
    }

    private void handleAccepted(IngestResult result) {
        result.acceptedRecord().ifPresent(record -> {
            TelemetryRecord stored = buffer.append(record);
            logger.accept("[gateway] buffered recordId=" + stored.recordId()
                    + " device=" + stored.deviceId() + " metric=" + stored.metric()
                    + " quality=" + stored.quality() + " pending=" + buffer.stats().pendingCount());
            alertEngine.evaluate(stored).ifPresent(e ->
                    logger.accept("[gateway] alert fired key=" + e.eventKey()));
            if (link.isAvailable()) {
                UplinkResult up = replayService.deliverOne(stored);
                logger.accept("[gateway] immediate uplink recordId=" + stored.recordId() + " -> " + up);
            } else {
                logger.accept("[gateway] link down, kept for replay recordId=" + stored.recordId());
            }
        });
    }

    /** 链路恢复：按序补传全部积压。 */
    public int onLinkRestored() {
        logger.accept("[gateway] link restored, replay begin pending="
                + buffer.stats().pendingCount());
        int delivered = replayService.replayPending();
        logger.accept("[gateway] replay done, delivered=" + delivered
                + " remaining=" + buffer.stats().pendingCount());
        return delivered;
    }

    private void logIngest(String kind, String raw, IngestResult result) {
        switch (result.status()) {
            case REJECTED -> logger.accept("[ingest] REJECTED kind=" + kind
                    + " reason=" + result.reason() + " detail=" + result.detail()
                    + " input=" + raw);
            case DEGRADED -> logger.accept("[ingest] DEGRADED kind=" + kind
                    + " detail=" + result.detail() + " input=" + raw);
            case ACCEPTED -> logger.accept("[ingest] ACCEPTED kind=" + kind + " input=" + raw);
        }
    }

    /** 测试/演示用：让调用方直接触发一次补传。 */
    public List<UplinkResult> forceReplay() {
        List<UplinkResult> results = new ArrayList<>();
        for (TelemetryRecord r : buffer.pendingRecords()) {
            results.add(replayService.deliverOne(r));
        }
        return results;
    }

    public BufferStats bufferStats() {
        return buffer.stats();
    }

    public AlertEngine alertEngine() {
        return alertEngine;
    }
}
