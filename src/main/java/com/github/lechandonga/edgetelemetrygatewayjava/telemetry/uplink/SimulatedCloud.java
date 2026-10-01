package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地模拟云端：维护已接收幂等键集合，天然对重复上报去重；
 * 可用开关模拟断连；丢包开关模拟“云端收到但 ACK 丢失”的抖动。
 */
public class SimulatedCloud implements CloudEndpoint {

    private static final Logger log = LoggerFactory.getLogger(SimulatedCloud.class);

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicBoolean dropNextAck = new AtomicBoolean(false);
    private final Set<String> receivedKeys = new HashSet<>();
    private final List<String> arrivalOrder = new CopyOnWriteArrayList<>();

    public void setAvailable(boolean up) {
        boolean old = available.getAndSet(up);
        log.info("[CLOUD] 链路状态切换: {} -> {}", old ? "UP" : "DOWN", up ? "UP" : "DOWN");
    }

    /** 下一次 receive 模拟 ACK 丢失：数据会被云端收下，但对网关表现为失败。 */
    public void simulateAckLossOnce() {
        dropNextAck.set(true);
    }

    @Override
    public synchronized Optional<String> receive(TelemetryRecord record) {
        if (!available.get()) {
            return Optional.empty();
        }
        boolean firstTime = receivedKeys.add(record.idempotencyKey());
        if (firstTime) {
            arrivalOrder.add(record.idempotencyKey());
        }
        log.info("[CLOUD] 收到 device={} metric={} ts={} 重复={}",
                record.deviceId(), record.metric(), record.timestamp(), !firstTime);
        if (dropNextAck.compareAndSet(true, false)) {
            log.warn("[CLOUD] 模拟 ACK 丢失 key={}", record.idempotencyKey());
            return Optional.empty();
        }
        return Optional.of("ACK-" + record.idempotencyKey());
    }

    @Override
    public boolean isAvailable() {
        return available.get();
    }

    @Override
    public synchronized int acceptedCount() {
        return receivedKeys.size();
    }

    public synchronized boolean hasReceived(String key) {
        return receivedKeys.contains(key);
    }

    public List<String> arrivalOrder() {
        return List.copyOf(arrivalOrder);
    }
}
