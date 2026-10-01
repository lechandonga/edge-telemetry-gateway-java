package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 简单的仅追加 JSONL 日志（本地持久化）。每行一个 JSON 对象，
 * 进程重启后通过 {@link #readAll(Class)} 全量回放恢复状态。
 */
public class Journal<T> {

    private final Path file;
    private final Class<T> type;

    public Journal(Path file, Class<T> type) {
        this.file = file;
        this.type = type;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized void append(T entry) {
        String line = Json.write(entry);
        try {
            Files.writeString(file, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized List<T> readAll() {
        if (!Files.exists(file)) {
            return new ArrayList<>();
        }
        try {
            List<T> result = new ArrayList<>();
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    result.add(Json.read(line, type));
                }
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized void rewrite(List<T> entries) {
        try {
            StringBuilder sb = new StringBuilder();
            for (T entry : entries) {
                sb.append(Json.write(entry)).append(System.lineSeparator());
            }
            Files.writeString(file, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path path() {
        return file;
    }
}
