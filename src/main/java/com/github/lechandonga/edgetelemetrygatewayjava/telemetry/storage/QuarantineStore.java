package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 永久拒收隔离区（quarantine），落盘文件 {@code quarantine.jsonl}。
 *
 * 语义：
 * - 记录被云端明确永久拒收（超时/指标下线等）时进入隔离区，从补传队列移除，
 *   后面排队的正常数据继续按序补传；从序列中间移除不影响前后数据的相对顺序。
 * - 按幂等键去重：同一条数据无论云端重复多少次永久拒收、或在“已隔离但缓冲 WAL
 *   尚未重写”的崩溃窗口内重启，都只会保留一份隔离记录，可精确对账。
 * - 仅追加持久化，进程重启后完整回放，仍可按原因分类查询。
 */
public class QuarantineStore {

    private static final Logger log = LoggerFactory.getLogger(QuarantineStore.class);

    private final Journal<QuarantineEntry> journal;
    private final Map<String, QuarantineEntry> byKey = new LinkedHashMap<>();

    public QuarantineStore(Path dataDir) {
        this.journal = new Journal<>(dataDir.resolve("quarantine.jsonl"), QuarantineEntry.class);
        for (QuarantineEntry entry : journal.readAll()) {
            if (byKey.put(entry.idempotencyKey(), entry) != null) {
                log.warn("[QUARANTINE] 回放时忽略重复隔离记录 key={}", entry.idempotencyKey());
            }
        }
    }

    /**
     * 隔离一条记录。
     *
     * @return true 表示新隔离；false 表示该 key 已在隔离区（重复拒收，忽略）
     */
    public synchronized boolean quarantine(TelemetryRecord record, CloudRejectReason reason,
                                           String detail) {
        String key = record.idempotencyKey();
        if (byKey.containsKey(key)) {
            log.warn("[QUARANTINE] key={} 已在隔离区，忽略重复隔离通知 reason={}", key, reason);
            return false;
        }
        QuarantineEntry entry = new QuarantineEntry(System.currentTimeMillis(), reason, detail, record);
        byKey.put(key, entry);
        journal.append(entry);
        log.warn("[QUARANTINE] 隔离永久拒收记录 key={} reason={} detail={} 隔离区总数={}",
                key, reason, detail, byKey.size());
        return true;
    }

    /** 隔离记录按隔离先后顺序的快照（去重后）。 */
    public synchronized List<QuarantineEntry> all() {
        return new ArrayList<>(byKey.values());
    }

    public synchronized List<QuarantineEntry> byReason(CloudRejectReason reason) {
        return byKey.values().stream().filter(e -> e.reason() == reason).toList();
    }

    public synchronized boolean containsKey(String key) {
        return byKey.containsKey(key);
    }

    public synchronized int size() {
        return byKey.size();
    }

    /** 各原因分类计数，对账与运维查询用。 */
    public synchronized Map<CloudRejectReason, Integer> countsByReason() {
        Map<CloudRejectReason, Integer> counts = new java.util.EnumMap<>(CloudRejectReason.class);
        for (CloudRejectReason reason : CloudRejectReason.values()) {
            counts.put(reason, 0);
        }
        for (QuarantineEntry entry : byKey.values()) {
            counts.merge(entry.reason(), 1, Integer::sum);
        }
        return counts;
    }

    /** 隔离区幂等键集合（恢复时用于识别崩溃窗口内已隔离的队头数据）。 */
    public synchronized Set<String> keySet() {
        return new HashSet<>(byKey.keySet());
    }
}
