package com.github.lechandonga.edgetelemetrygatewayjava.alert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 带版本的告警规则库。规则不可变，更新 = 追加更高版本。持久化到本地。
 * 同一段历史数据用同一版本规则重放，必然得到相同结论。
 */
public class RuleStore {

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final CopyOnWriteArrayList<AlertRule> rules = new CopyOnWriteArrayList<>();

    public RuleStore(Path stateDir) {
        try {
            Files.createDirectories(stateDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.file = stateDir.resolve("alert-rules.json");
        load();
    }

    /** 发布新版本规则；version 自动取该指标最大值 + 1。 */
    public synchronized AlertRule publish(String metric, double gtThreshold,
                                          long durationMs, long createdAtMs) {
        int nextVersion = rulesFor(metric).stream()
                .mapToInt(AlertRule::version).max().orElse(0) + 1;
        AlertRule rule = new AlertRule(metric, gtThreshold, durationMs, nextVersion, createdAtMs);
        rules.add(rule);
        persist();
        return rule;
    }

    public List<AlertRule> rules() {
        return new ArrayList<>(rules);
    }

    public List<AlertRule> rulesFor(String metric) {
        return rules.stream().filter(r -> r.metric().equals(metric))
                .sorted(Comparator.comparingInt(AlertRule::version)).toList();
    }

    public Optional<AlertRule> findVersion(String metric, int version) {
        return rules.stream().filter(r -> r.metric().equals(metric) && r.version() == version)
                .findFirst();
    }

    public Optional<AlertRule> latest(String metric) {
        return rules.stream().filter(r -> r.metric().equals(metric))
                .max(Comparator.comparingInt(AlertRule::version));
    }

    private void load() {
        if (!Files.exists(file)) {
            return;
        }
        try {
            List<AlertRule> loaded = mapper.readValue(
                    Files.readString(file, StandardCharsets.UTF_8),
                    new TypeReference<List<AlertRule>>() {});
            rules.addAll(loaded);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling("alert-rules.json.tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(rules), StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
