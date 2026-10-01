package com.github.lechandonga.edgetelemetrygatewayjava.alert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** 已触发告警的持久化审计存储，按 eventKey 去重。 */
public class AlertEventStore {

    private final Path file;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Set<String> keys = new LinkedHashSet<>();
    private final List<AlertEvent> events = new ArrayList<>();

    public AlertEventStore(Path stateDir) {
        try {
            Files.createDirectories(stateDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.file = stateDir.resolve("alert-events.json");
        load();
    }

    /** @return true 首次记录；false 重复（不追加）。 */
    public synchronized boolean recordIfNew(AlertEvent event) {
        if (!keys.add(event.eventKey())) {
            return false;
        }
        events.add(event);
        persist();
        return true;
    }

    public synchronized List<AlertEvent> events() {
        return new ArrayList<>(events);
    }

    private void load() {
        if (!Files.exists(file)) {
            return;
        }
        try {
            List<AlertEvent> loaded = mapper.readValue(
                    Files.readString(file, StandardCharsets.UTF_8),
                    new TypeReference<List<AlertEvent>>() {});
            for (AlertEvent e : loaded) {
                keys.add(e.eventKey());
                events.add(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void persist() {
        try {
            Path tmp = file.resolveSibling("alert-events.json.tmp");
            Files.writeString(tmp, mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(events), StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
