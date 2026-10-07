package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 云端已确认接收键的本地仅追加账本（accepted-keys.jsonl）。
 *
 * 用途：对账时“云端已接收”以网关本地持久化证据为准，不依赖云端内存状态，
 * 因此进程重启（哪怕对端是模拟实例）后总账依然精确。
 * 键去重：重发、重复 ACK、崩溃恢复边界都不会重复记账。
 */
public class AcceptedLedger {

    private final Journal<String> journal;
    private final Set<String> keys = new HashSet<>();

    public AcceptedLedger(Path dataDir) {
        this.journal = new Journal<>(dataDir.resolve("accepted-keys.jsonl"), String.class);
        keys.addAll(journal.readAll());
    }

    /** @return true 表示新记账；false 表示重复确认（幂等忽略，不重复计数）。 */
    public synchronized boolean record(String idempotencyKey) {
        if (!keys.add(idempotencyKey)) {
            return false;
        }
        journal.append(idempotencyKey);
        return true;
    }

    public synchronized boolean containsKey(String key) {
        return keys.contains(key);
    }

    public synchronized int size() {
        return keys.size();
    }

    public synchronized List<String> keys() {
        return List.copyOf(keys);
    }
}
