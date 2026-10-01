package com.github.lechandonga.edgetelemetrygatewayjava.alert;

import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertEvent;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertRule;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 无状态历史重放：同一段记录 + 同一版本规则，必然得到相同事件序列。
 * 判定算法与 {@link AlertEngine} 一致，但不读写任何持久状态，可对任意历史版本复现结论。
 */
public final class AlertReplay {

    private AlertReplay() {
    }

    private record Window(boolean active, long start, double max, long firstId, boolean fired) {
    }

    public static List<AlertEvent> replay(List<TelemetryRecord> records, AlertRule rule) {
        List<AlertEvent> out = new ArrayList<>();
        Set<String> seenKeys = new HashSet<>();
        Map<String, Window> windows = new HashMap<>();

        List<TelemetryRecord> sorted = records.stream()
                .filter(r -> r.metric().equals(rule.metric()))
                .sorted(Comparator.comparingLong(TelemetryRecord::timestampMs)
                        .thenComparingLong(TelemetryRecord::deviceSeq))
                .toList();

        for (TelemetryRecord r : sorted) {
            String k = r.deviceId() + "|" + r.metric();
            Window w = windows.getOrDefault(k, new Window(false, 0L, 0d, -1L, false));

            if (!rule.breached(r.value())) {
                windows.put(k, new Window(false, 0L, 0d, -1L, false));
                continue;
            }
            if (!w.active()) {
                windows.put(k, new Window(true, r.timestampMs(), r.value(), r.recordId(), false));
                continue;
            }
            double max = Math.max(w.max(), r.value());
            Window next = new Window(true, w.start(), max, w.firstId(), w.fired());
            if (!w.fired() && r.timestampMs() - w.start() >= rule.durationMs()) {
                AlertEvent event = new AlertEvent(
                        AlertEvent.key(r.deviceId(), r.metric(), rule.version(), w.start()),
                        r.deviceId(), r.metric(), rule.version(), w.start(),
                        r.timestampMs(), max, w.firstId(), r.recordId());
                if (seenKeys.add(event.eventKey())) {
                    out.add(event);
                }
                next = new Window(true, w.start(), max, w.firstId(), true);
            }
            windows.put(k, next);
        }
        return out;
    }
}
