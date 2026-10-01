package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.MetricSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.IngestResult;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.Quality;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.RejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.DeadLetterLog;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 采集归一化入口。两种上报形式（JSON 报文、寄存器帧）在此统一为
 * {@link TelemetryRecord}，并执行：字段完整性 → 设备登记 → 指标声明 →
 * 数值有效性 → 硬量程 → 每设备时间戳顺序 → 幂等去重。
 *
 * 硬错误：拒收并写死信日志（可区分原因）；
 * 软异常（超出可信区间）：降级为 DEGRADED 但保留数据。
 *
 * 每设备一把顺序锁：同一设备的校验与接收原子完成，并发上报下设备内
 * 时间戳顺序稳定，lastTimestamp 不会因竞态回退。
 */
public class Normalizer {

    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    private final DeviceRegistry registry;
    private final DeadLetterLog deadLetterLog;

    private final Map<String, Object> deviceLocks = new HashMap<>();
    private final Map<String, Long> lastTimestampPerDeviceMetric = new HashMap<>();
    private final Map<String, Long> sequencePerDevice = new HashMap<>();
    private final Map<String, String> lastKeyPerDeviceMetric = new HashMap<>();

    public Normalizer(DeviceRegistry registry, DeadLetterLog deadLetterLog) {
        this.registry = registry;
        this.deadLetterLog = deadLetterLog;
    }

    /** 重启恢复设备水位线，保证重启后顺序与去重判定延续。 */
    public synchronized void restoreWatermark(String deviceId, String metric, long timestamp) {
        lastTimestampPerDeviceMetric.merge(deviceId + "|" + metric, timestamp, Math::max);
    }

    public synchronized IngestResult ingestJson(String rawJson) {
        JsonNode node;
        try {
            node = Json.MAPPER.readTree(rawJson);
        } catch (Exception e) {
            return reject("JSON", rawJson, RejectReason.MALFORMED, "JSON 解析失败: " + e.getMessage());
        }
        String deviceId = text(node, "deviceId");
        String metric = text(node, "metric");
        JsonNode valueNode = node.get("value");
        Long timestamp = longField(node, "timestamp");

        IngestResult pre = precheck("JSON", rawJson, deviceId, metric, valueNode, timestamp);
        if (pre != null) {
            return pre;
        }
        double value;
        try {
            value = parseValue(valueNode);
        } catch (NumberFormatException e) {
            return reject("JSON", rawJson, RejectReason.INVALID_VALUE, e.getMessage());
        }
        return accept("JSON", rawJson, deviceId, metric, value, timestamp);
    }

    public IngestResult ingestRegister(RegisterFrame frame) {
        String raw = frame.toString();
        if (frame.deviceId() == null || frame.deviceId().isBlank()) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.MISSING_FIELD, "寄存器帧缺少 deviceId");
        }
        if (frame.address() == null || frame.address().isBlank()) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.MISSING_FIELD, "寄存器帧缺少 address");
        }
        if (frame.rawValue() == null || frame.rawValue().isBlank()) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.MISSING_FIELD, "寄存器帧缺少 value");
        }
        if (frame.timestamp() == null) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.MISSING_FIELD, "寄存器帧缺少 timestamp");
        }
        Optional<DeviceSpec> device = registry.find(frame.deviceId());
        if (device.isEmpty()) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.UNKNOWN_DEVICE,
                    "未登记设备: " + frame.deviceId());
        }
        String metric = device.get().resolveRegister(frame.address());
        if (metric == null) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.UNKNOWN_METRIC,
                    "设备 " + frame.deviceId() + " 未声明寄存器 " + frame.address());
        }
        double value;
        try {
            value = parseValue(Json.MAPPER.getNodeFactory().textNode(frame.rawValue()));
        } catch (NumberFormatException e) {
            return reject(RegisterFrame.SOURCE, raw, RejectReason.INVALID_VALUE, e.getMessage());
        }
        return accept(RegisterFrame.SOURCE, raw, frame.deviceId(), metric, value, frame.timestamp());
    }

    private IngestResult precheck(String source, String raw, String deviceId, String metric,
                                  JsonNode valueNode, Long timestamp) {
        if (deviceId == null || deviceId.isBlank()) {
            return reject(source, raw, RejectReason.MISSING_FIELD, "缺少 deviceId");
        }
        if (metric == null || metric.isBlank()) {
            return reject(source, raw, RejectReason.MISSING_FIELD, "缺少 metric");
        }
        if (valueNode == null || valueNode.isNull()) {
            return reject(source, raw, RejectReason.MISSING_FIELD, "缺少 value");
        }
        if (timestamp == null) {
            return reject(source, raw, RejectReason.MISSING_FIELD, "缺少 timestamp 或格式非法");
        }
        if (registry.find(deviceId).isEmpty()) {
            return reject(source, raw, RejectReason.UNKNOWN_DEVICE, "未登记设备: " + deviceId);
        }
        if (registry.find(deviceId).get().metric(metric) == null) {
            return reject(source, raw, RejectReason.UNKNOWN_METRIC,
                    "设备 " + deviceId + " 未声明指标 " + metric);
        }
        return null;
    }

    private IngestResult accept(String source, String raw, String deviceId, String metric,
                                double value, long timestamp) {
        DeviceSpec device = registry.find(deviceId).orElseThrow();
        MetricSpec spec = device.metric(metric);
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return reject(source, raw, RejectReason.INVALID_VALUE, "数值为 NaN/Infinity");
        }
        if (spec.outsideHardRange(value)) {
            return reject(source, raw, RejectReason.OUT_OF_RANGE,
                    String.format("值 %s 超出硬量程 [%s,%s]", value, spec.hardMin(), spec.hardMax()));
        }
        List<String> degrades = new ArrayList<>();
        if (spec.outsideSoftRange(value)) {
            degrades.add(String.format("值 %s 超出可信区间 [%s,%s]", value, spec.softMin(), spec.softMax()));
        }
        Object lock;
        synchronized (this) {
            lock = deviceLocks.computeIfAbsent(deviceId, k -> new Object());
        }
        synchronized (lock) {
            String watermarkKey = deviceId + "|" + metric;
            Long last = lastTimestampPerDeviceMetric.get(watermarkKey);
            if (last != null && timestamp < last) {
                return reject(source, raw, RejectReason.OUT_OF_ORDER,
                        "时间戳 " + timestamp + " 早于设备已接收水位线 " + last);
            }
            long seq = sequencePerDevice.merge(deviceId, 1L, Long::sum);
            TelemetryRecord record = new TelemetryRecord(seq, deviceId, metric, value, timestamp,
                    source, degrades.isEmpty() ? Quality.GOOD : Quality.DEGRADED, degrades);
            String key = record.idempotencyKey();
            if (key.equals(lastKeyPerDeviceMetric.get(watermarkKey))) {
                return reject(source, raw, RejectReason.DUPLICATE, "幂等键重复: " + key);
            }
            lastKeyPerDeviceMetric.put(watermarkKey, key);
            lastTimestampPerDeviceMetric.put(watermarkKey, timestamp);
            log.info("[INGEST] 接收 device={} metric={} value={} ts={} quality={} seq={} 依据={}",
                    deviceId, metric, value, timestamp, record.quality(), seq,
                    degrades.isEmpty() ? "全部校验通过" : degrades);
            return IngestResult.ok(record);
        }
    }

    private IngestResult reject(String source, String raw, RejectReason reason, String detail) {
        log.warn("[INGEST] 拒收 reason={} detail={} raw={}", reason, detail, raw);
        deadLetterLog.record(source, raw, reason, detail);
        return IngestResult.reject(reason, detail);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static Long longField(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || !v.isNumber()) {
            return null;
        }
        return v.asLong();
    }

    private static double parseValue(JsonNode node) {
        if (node.isNumber()) {
            return node.asDouble();
        }
        String text = node.asText();
        try {
            double v = Double.parseDouble(text.trim());
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new NumberFormatException("NaN/Infinity");
            }
            return v;
        } catch (NumberFormatException e) {
            throw new NumberFormatException("无法解析为数值: " + text);
        }
    }
}
