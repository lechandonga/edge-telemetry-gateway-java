package com.github.lechandonga.edgetelemetrygatewayjava.alert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertEvent;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertRule;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * 阈值持续时长告警引擎（有状态、可持久化、跨重启恢复）。
 *
 * 判定规则：同一 (device, metric) 的值连续处于越限态达到 rule.durationMs 触发一次；
 * 一旦出现未越限样本，窗口立即复位。反复越限/恢复会产生独立窗口，各自 windowStart/eventKey；
 * 已触发窗口在复位前不会重复触发。
 *
 * 规则版本：已开窗的窗口绑定开窗时版本并沿用到底；新窗口取最新版本，
 * 因此规则更新不会丢弃或重复触发在途窗口。
 */
public class AlertEngine {

    private final RuleStore ruleStore;
    private final AlertEventStore eventStore;
    private final Path stateFile;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Consumer<String> logger;
    private final Map<String, AlertState> states = new HashMap<>();

    public AlertEngine(RuleStore ruleStore, AlertEventStore eventStore, Path stateDir,
                       Consumer<String> logger) {
        this.ruleStore = ruleStore;
        this.eventStore = eventStore;
        this.logger = logger != null ? logger : s -> {};
        try {
            Files.createDirectories(stateDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.stateFile = stateDir.resolve("alert-state.json");
        load();
    }

    /**
     * 评估一条记录。
     * @return 新触发的告警；无触发（含重复被抑制）返回 empty。
     */
    public synchronized Optional<AlertEvent> evaluate(TelemetryRecord r) {
        String key = r.deviceId() + "|" + r.metric();
        AlertState state = states.getOrDefault(key, AlertState.empty(r.deviceId(), r.metric()));

        AlertRule rule;
        if (state.active()) {
            rule = ruleStore.findVersion(r.metric(), state.ruleVersion())
                    .orElseGet(() -> ruleStore.latest(r.metric()).orElse(null));
        } else {
            rule = ruleStore.latest(r.metric()).orElse(null);
        }
        if (rule == null) {
            return Optional.empty();
        }

        boolean breached = rule.breached(r.value());
        AlertState next;
        AlertEvent firedEvent = null;

        if (!breached) {
            if (state.active()) {
                logger.accept(String.format(
                        "[alert] reset device=%s metric=%s v%d windowStart=%d fired=%s value=%s ts=%d",
                        r.deviceId(), r.metric(), state.ruleVersion(), state.windowStartMs(),
                        state.fired(), r.value(), r.timestampMs()));
            }
            next = AlertState.empty(r.deviceId(), r.metric());
        } else if (!state.active()) {
            next = state.with(rule.version(), true, r.timestampMs(), r.value(),
                    r.recordId(), false);
            logger.accept(String.format(
                    "[alert] window-open device=%s metric=%s v%d start=%d value=%s",
                    r.deviceId(), r.metric(), rule.version(), r.timestampMs(), r.value()));
        } else {
            double max = Math.max(state.maxValue(), r.value());
            next = state.with(state.ruleVersion(), true, state.windowStartMs(), max,
                    state.firstRecordId(), state.fired());
            if (!state.fired() && r.timestampMs() - state.windowStartMs() >= rule.durationMs()) {
                AlertEvent event = new AlertEvent(
                        AlertEvent.key(r.deviceId(), r.metric(), rule.version(),
                                state.windowStartMs()),
                        r.deviceId(), r.metric(), rule.version(), state.windowStartMs(),
                        r.timestampMs(), max, state.firstRecordId(), r.recordId());
                if (eventStore.recordIfNew(event)) {
                    firedEvent = event;
                    logger.accept(String.format(
                            "[alert] FIRE key=%s windowStart=%d firedAt=%d max=%s v%d",
                            event.eventKey(), event.windowStartMs(), event.firedAtMs(),
                            event.maxValueInWindow(), rule.version()));
                } else {
                    logger.accept("[alert] suppressed duplicate key=" + event.eventKey());
                }
                next = next.with(rule.version(), true, state.windowStartMs(), max,
                        state.firstRecordId(), true);
            }
        }

        states.put(key, next);
        persist();
        return Optional.ofNullable(firedEvent);
    }

    public synchronized List<AlertEvent> firedEvents() {
        return eventStore.events();
    }

    private void load() {
        if (!Files.exists(stateFile)) {
            return;
        }
        try {
            List<AlertState> loaded = mapper.readValue(
                    Files.readString(stateFile, StandardCharsets.UTF_8),
                    new TypeReference<List<AlertState>>() {});
            for (AlertState s : loaded) {
                states.put(s.deviceId() + "|" + s.metric(), s);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void persist() {
        try {
            Path tmp = stateFile.resolveSibling("alert-state.json.tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(new ArrayList<>(states.values())), StandardCharsets.UTF_8);
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
