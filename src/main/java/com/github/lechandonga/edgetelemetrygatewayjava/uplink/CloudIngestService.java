package com.github.lechandonga.edgetelemetrygatewayjava.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 云端本地模拟。云端自身也按 recordId 去重：即使重复补传/重复确认发生，
 * 云端存储仍不重不漏。
 */
public class CloudIngestService {

    private final Set<Long> ingestedIds = new LinkedHashSet<>();
    private final List<TelemetryRecord> received = new ArrayList<>();

    /** @return true 首次接收；false 重复（已存在）。 */
    public synchronized boolean ingest(TelemetryRecord record) {
        if (!ingestedIds.add(record.recordId())) {
            return false;
        }
        received.add(record);
        return true;
    }

    public synchronized List<TelemetryRecord> received() {
        return new ArrayList<>(received);
    }

    public synchronized int receivedCount() {
        return received.size();
    }

    public synchronized void reset() {
        ingestedIds.clear();
        received.clear();
    }
}
