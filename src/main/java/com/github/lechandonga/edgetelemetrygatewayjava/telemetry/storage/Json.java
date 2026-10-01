package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * 共享 Jackson 配置。所有持久化文件均为可读 JSON / JSONL。
 */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private Json() {
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T read(String content, Class<T> type) {
        try {
            return MAPPER.readValue(content, type);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
