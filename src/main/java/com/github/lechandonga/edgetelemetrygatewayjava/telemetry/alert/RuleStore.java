package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Json;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * 规则配置存储：当前版本 + 全部历史版本（各存一个 JSON 文件）。
 * 同一段历史数据可用任一历史版本规则确定性重放。
 */
public class RuleStore {

    private final Path currentFile;
    private final Path versionsDir;

    public RuleStore(Path dataDir) {
        try {
            Files.createDirectories(dataDir.resolve("rule-versions"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.currentFile = dataDir.resolve("rules-current.json");
        this.versionsDir = dataDir.resolve("rule-versions");
    }

    /** 发布新版本：写入历史版本目录并更新 current 指针。 */
    public synchronized void publish(RuleSet ruleSet) {
        Path versionFile = versionsDir.resolve(ruleSet.version() + ".json");
        write(versionFile, ruleSet);
        write(currentFile, ruleSet);
    }

    public synchronized Optional<RuleSet> current() {
        return readIfExists(currentFile);
    }

    public synchronized Optional<RuleSet> version(String version) {
        return readIfExists(versionsDir.resolve(version + ".json"));
    }

    private void write(Path file, RuleSet ruleSet) {
        try {
            Files.writeString(file, Json.write(ruleSet));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Optional<RuleSet> readIfExists(Path file) {
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Json.read(Files.readString(file), RuleSet.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
