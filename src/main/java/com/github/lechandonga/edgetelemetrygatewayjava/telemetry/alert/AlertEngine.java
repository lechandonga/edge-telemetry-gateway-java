package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Json;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Journal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 持续时长阈值告警引擎。
 *
 * 防重复：规则状态机 NORMAL→PENDING→FIRED→（恢复采样）→NORMAL，
 * FIRED 阶段再多次越限不重复告警；先 RECOVER 后再次持续越限才产生新 FIRE。
 * 跨重启：每次状态迁移后整体快照落盘（alert-state.json，原子替换），
 * 告警事件仅追加（alert-events.jsonl），重启恢复后不会重复触发。
 * 规则版本：事件携带 ruleVersion；规则更新时按 ruleId 携带已进入的窗口状态，
 * 既不丢弃窗口也不会因状态归零而重复触发。
 */
public class AlertEngine {

    private static final Logger log = LoggerFactory.getLogger(AlertEngine.class);

    private final RuleStore ruleStore;
    private final Path snapshotFile;
    private final Journal<AlertEvent> eventJournal;

    private RuleSet ruleSet;
    private Map<String, RuleState> states = new HashMap<>();
    private Map<String, ThresholdRule> ruleIndex = new HashMap<>();

    public AlertEngine(Path dataDir, RuleStore ruleStore) {
        this.ruleStore = ruleStore;
        this.snapshotFile = dataDir.resolve("alert-state.json");
        this.eventJournal = new Journal<>(dataDir.resolve("alert-events.jsonl"), AlertEvent.class);
        this.ruleSet = ruleStore.current().orElse(null);
        indexRules();
        loadSnapshot();
    }

    /**
     * 处理一条已接受遥测，返回新产生的告警事件（通常 0 或 1 条）。
     */
    public synchronized List<AlertEvent> process(TelemetryRecord record) {
        List<AlertEvent> events = new ArrayList<>();
        if (ruleSet == null) {
            return events;
        }
        for (ThresholdRule rule : ruleSet.rules()) {
            if (!rule.deviceId().equals(record.deviceId()) || !rule.metric().equals(record.metric())) {
                continue;
            }
            String key = rule.stateKey();
            RuleState state = states.computeIfAbsent(key,
                    k -> new RuleState(RuleState.NORMAL, ruleSet.version(), 0, 0, 0));
            boolean violated = rule.operator().violated(record.value(), rule.threshold());
            long now = record.timestamp();
            if (violated) {
                switch (state.phase) {
                    case RuleState.NORMAL -> {
                        state.phase = RuleState.PENDING;
                        state.ruleVersion = ruleSet.version();
                        state.windowStart = now;
                        log.info("[ALERT] 规则 {} 越限进入观察窗口 start={} value={} version={}",
                                rule.ruleId(), now, record.value(), ruleSet.version());
                    }
                    case RuleState.PENDING -> {
                        if (now - state.windowStart >= rule.durationMillis()) {
                            state.phase = RuleState.FIRED;
                            AlertEvent event = fire(rule, state, record);
                            events.add(event);
                            if (eventJournal != null) {
                                eventJournal.append(event);
                            }
                            log.warn("[ALERT] FIRE rule={} 窗口 {}-{} 持续 {}ms value={}",
                                    rule.ruleId(), state.windowStart, now,
                                    now - state.windowStart, record.value());
                        }
                    }
                    case RuleState.FIRED -> log.debug("[ALERT] 规则 {} 持续越限，已告警不重复触发",
                            rule.ruleId());
                    default -> { }
                }
            } else if (RuleState.FIRED.equals(state.phase)) {
                AlertEvent event = recover(rule, state, record);
                events.add(event);
                if (eventJournal != null) {
                    eventJournal.append(event);
                }
                log.info("[ALERT] RECOVER rule={} ts={} value={}", rule.ruleId(), now, record.value());
                resetState(state);
            } else if (RuleState.PENDING.equals(state.phase)) {
                log.info("[ALERT] 规则 {} 窗口内提前恢复，窗口作废 start={}",
                        rule.ruleId(), state.windowStart);
                resetState(state);
            }
            state.lastSeenTimestamp = now;
            state.lastValue = record.value();
        }
        if (!events.isEmpty() || !states.isEmpty()) {
            persistSnapshot();
        }
        return events;
    }

    private AlertEvent fire(ThresholdRule rule, RuleState state, TelemetryRecord record) {
        return new AlertEvent(AlertEvent.FIRE, rule.ruleId(), ruleSet.version(),
                rule.deviceId(), rule.metric(), record.value(), record.timestamp(),
                state.windowStart, record.timestamp() - state.windowStart,
                String.format("%s %s %s 且持续 %dms", rule.metric(), rule.operator(),
                        rule.threshold(), rule.durationMillis()));
    }

    private AlertEvent recover(ThresholdRule rule, RuleState state, TelemetryRecord record) {
        return new AlertEvent(AlertEvent.RECOVER, rule.ruleId(), state.ruleVersion,
                rule.deviceId(), rule.metric(), record.value(), record.timestamp(),
                state.windowStart, record.timestamp() - state.windowStart,
                String.format("%s 恢复至 %s %s", rule.metric(), rule.operator(), rule.threshold()));
    }

    private void resetState(RuleState state) {
        state.phase = RuleState.NORMAL;
        state.windowStart = 0;
    }

    /**
     * 发布新版规则：按 ruleId 携带已进入的窗口状态（更新规则版本标记），
     * 新版中删除的规则状态随之下线。窗口不重置，因此不会丢弃判定或重复触发。
     */
    public synchronized void publishRules(RuleSet next) {
        ruleStore.publish(next);
        Map<String, RuleState> migrated = new HashMap<>();
        Map<String, Integer> carried = new HashMap<>();
        for (ThresholdRule rule : next.rules()) {
            RuleState old = states.get(rule.stateKey());
            if (old != null && !RuleState.NORMAL.equals(old.phase)) {
                old.ruleVersion = next.version();
                migrated.put(rule.stateKey(), old);
                carried.merge(old.phase, 1, Integer::sum);
            }
        }
        this.ruleSet = next;
        indexRules();
        this.states = migrated;
        persistSnapshot();
        log.info("[ALERT] 规则版本切换 -> {}，迁移进行中窗口: {}，其余状态归零",
                next.version(), carried);
    }

    public synchronized RuleSet currentRules() {
        return ruleSet;
    }

    public synchronized List<AlertEvent> events() {
        return eventJournal.readAll();
    }

    /**
     * 用指定版本规则对历史数据做确定性重放：全新状态、按时间戳稳定排序、
     * 无副作用。同一 (规则版本, 历史数据集合) 必然得到相同结论。
     */
    public static List<AlertEvent> replay(List<TelemetryRecord> history, RuleSet rules) {
        AlertEngine ephemeral = new EphemeralEngine(rules);
        List<TelemetryRecord> ordered = new ArrayList<>(history);
        ordered.sort(Comparator.comparingLong(TelemetryRecord::timestamp)
                .thenComparing(TelemetryRecord::deviceId)
                .thenComparing(TelemetryRecord::metric));
        List<AlertEvent> result = new ArrayList<>();
        for (TelemetryRecord record : ordered) {
            result.addAll(ephemeral.dispatch(record));
        }
        return result;
    }

    private List<AlertEvent> dispatch(TelemetryRecord record) {
        return process(record);
    }

    private static class EphemeralEngine extends AlertEngine {
        EphemeralEngine(RuleSet rules) {
            super(rules);
        }
    }

    private AlertEngine(RuleSet ephemeralRules) {
        this.ruleStore = null;
        this.snapshotFile = null;
        this.eventJournal = null;
        this.ruleSet = ephemeralRules;
        indexRules();
    }

    private void indexRules() {
        ruleIndex.clear();
        if (ruleSet != null) {
            for (ThresholdRule rule : ruleSet.rules()) {
                ruleIndex.put(rule.stateKey(), rule);
            }
        }
    }

    private void loadSnapshot() {
        if (!Files.exists(snapshotFile)) {
            return;
        }
        try {
            StateSnapshot snapshot = Json.read(Files.readString(snapshotFile), StateSnapshot.class);
            if (ruleSet != null && ruleSet.version().equals(snapshot.currentVersion)) {
                this.states = snapshot.states == null ? new HashMap<>() : snapshot.states;
                log.info("[ALERT] 重启恢复告警状态 version={} 非NORMAL窗口数={}",
                        snapshot.currentVersion,
                        states.values().stream().filter(s -> !RuleState.NORMAL.equals(s.phase)).count());
            } else {
                log.info("[ALERT] 快照版本 {} 与当前规则 {} 不一致，窗口状态不沿用",
                        snapshot.currentVersion, ruleSet == null ? null : ruleSet.version());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void persistSnapshot() {
        if (snapshotFile == null) {
            return;
        }
        try {
            StateSnapshot snapshot = new StateSnapshot();
            snapshot.currentVersion = ruleSet == null ? null : ruleSet.version();
            snapshot.states = new HashMap<>(states);
            Path tmp = snapshotFile.resolveSibling(snapshotFile.getFileName() + ".tmp");
            Files.writeString(tmp, Json.write(snapshot));
            Files.move(tmp, snapshotFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
