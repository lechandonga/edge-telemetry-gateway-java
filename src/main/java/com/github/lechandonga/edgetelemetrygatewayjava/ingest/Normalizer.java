package com.github.lechandonga.edgetelemetrygatewayjava.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lechandonga.edgetelemetrygatewayjava.model.Quality;
import com.github.lechandonga.edgetelemetrygatewayjava.model.RejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.model.SourceType;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 把两种上报形式归一为 {@link TelemetryRecord}。
 *
 * 顺序保证：按 deviceId 加锁，维护每设备的单调 deviceSeq 与最新时间戳。
 * 乱序策略：CLAMP（默认，时间戳钳制为最新值并降级 TIMESTAMP_CLAMPED）或 REJECT（拒收 OUT_OF_ORDER）。
 */
public class Normalizer {

    public enum OutOfOrderPolicy { CLAMP, REJECT }

    private record DeviceCursor(long seq, long lastTs) {}

    private final DeviceRegistry registry;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Object> deviceLocks = new ConcurrentHashMap<>();
    private final Map<String, DeviceCursor> cursors = new ConcurrentHashMap<>();
    private volatile OutOfOrderPolicy policy;

    public Normalizer(DeviceRegistry registry) {
        this(registry, OutOfOrderPolicy.CLAMP);
    }

    public Normalizer(DeviceRegistry registry, OutOfOrderPolicy policy) {
        this.registry = registry;
        this.policy = policy;
    }

    public void setOutOfOrderPolicy(OutOfOrderPolicy policy) {
        this.policy = policy;
    }

    /** 归一化寄存器采样。 */
    public IngestResult normalize(RegisterSample sample) {
        if (sample == null || sample.deviceId() == null) {
            return IngestResult.rejected(RejectReason.MISSING_FIELD, "deviceId is null");
        }
        Optional<DeviceSpec> specOpt = registry.find(sample.deviceId());
        if (specOpt.isEmpty()) {
            return IngestResult.rejected(RejectReason.UNKNOWN_DEVICE,
                    "device not registered: " + sample.deviceId());
        }
        DeviceSpec spec = specOpt.get();
        String metric = spec.registerMap().get(sample.registerAddress());
        if (metric == null) {
            return IngestResult.rejected(RejectReason.MISSING_FIELD,
                    "register address not mapped: " + sample.registerAddress());
        }
        if (sample.value() == null || sample.timestampMs() == null) {
            return IngestResult.rejected(RejectReason.MISSING_FIELD, "value or timestamp is null");
        }
        return finish(spec, metric, sample.value(), sample.timestampMs(), SourceType.REGISTER_SAMPLE);
    }

    /** 归一化 JSON 报文，格式 {"deviceId","metric","value","ts"}。 */
    public IngestResult normalizeJson(String json) {
        JsonNode node;
        try {
            node = mapper.readTree(json);
        } catch (Exception e) {
            return IngestResult.rejected(RejectReason.MALFORMED_PAYLOAD, e.getMessage());
        }
        String deviceId = textOrNull(node, "deviceId");
        if (deviceId == null) {
            return IngestResult.rejected(RejectReason.MISSING_FIELD, "deviceId is null");
        }
        Optional<DeviceSpec> specOpt = registry.find(deviceId);
        if (specOpt.isEmpty()) {
            return IngestResult.rejected(RejectReason.UNKNOWN_DEVICE, "device not registered: " + deviceId);
        }
        String metric = textOrNull(node, "metric");
        JsonNode valueNode = node.get("value");
        JsonNode tsNode = node.get("ts");
        if (metric == null || valueNode == null || valueNode.isNull() || tsNode == null || tsNode.isNull()) {
            return IngestResult.rejected(RejectReason.MISSING_FIELD,
                    "one of metric/value/ts is missing");
        }
        double value;
        long ts;
        try {
            value = valueNode.asDouble();
            ts = tsNode.asLong();
        } catch (Exception e) {
            return IngestResult.rejected(RejectReason.MALFORMED_PAYLOAD, "value/ts not numeric: " + e.getMessage());
        }
        return finish(specOpt.get(), metric, value, ts, SourceType.JSON_REPORT);
    }

    private IngestResult finish(DeviceSpec spec, String metric, double value, long ts, SourceType source) {
        double[] range = spec.metricRanges().get(metric);
        if (range == null) {
            return IngestResult.rejected(RejectReason.MISSING_FIELD, "metric not declared for device: " + metric);
        }
        if (Double.isNaN(value) || Double.isInfinite(value) || value < range[0] || value > range[1]) {
            return IngestResult.rejected(RejectReason.VALUE_OUT_OF_RANGE,
                    "value " + value + " out of range [" + range[0] + "," + range[1] + "] for " + metric);
        }

        Object lock = deviceLocks.computeIfAbsent(spec.deviceId(), k -> new Object());
        synchronized (lock) {
            DeviceCursor cursor = cursors.getOrDefault(spec.deviceId(), new DeviceCursor(0, Long.MIN_VALUE));
            long effectiveTs = ts;
            Quality quality = Quality.GOOD;
            if (ts < cursor.lastTs()) {
                if (policy == OutOfOrderPolicy.REJECT) {
                    return IngestResult.rejected(RejectReason.OUT_OF_ORDER,
                            "ts " + ts + " < lastTs " + cursor.lastTs());
                }
                effectiveTs = cursor.lastTs();
                quality = Quality.TIMESTAMP_CLAMPED;
            }
            long nextSeq = cursor.seq() + 1;
            cursors.put(spec.deviceId(), new DeviceCursor(nextSeq, effectiveTs));
            TelemetryRecord record = new TelemetryRecord(
                    -1L, spec.deviceId(), metric, value, effectiveTs, nextSeq, source, quality);
            if (quality == Quality.TIMESTAMP_CLAMPED) {
                return IngestResult.degraded(record,
                        "out-of-order ts " + ts + " clamped to " + effectiveTs);
            }
            return IngestResult.accepted(record);
        }
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull()) {
            return null;
        }
        String text = v.asText();
        return text == null || text.isBlank() ? null : text;
    }
}
