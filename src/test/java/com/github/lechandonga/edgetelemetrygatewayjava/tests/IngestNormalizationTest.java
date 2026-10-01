package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.IngestResult;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.Quality;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.RejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.GatewayTestSupport;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.ScenarioLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 两种上报形式归一化 + 错误分类 + 顺序/去重 + 降级。
 */
class IngestNormalizationTest {

    private final TelemetryGateway[] holder = new TelemetryGateway[1];

    private String json(String device, String metric, Object value, long ts) {
        String v = value instanceof String s ? "\"" + s + "\"" : String.valueOf(value);
        return String.format("{\"deviceId\":\"%s\",\"metric\":\"%s\",\"value\":%s,\"timestamp\":%d}",
                device, metric, v, ts);
    }

    @Test
    void jsonAndRegisterNormalizeToSameModel(@TempDir Path dir) {
        GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        IngestResult j = gw.ingestJson(json("dev-001", "temperature", 25.0, 1000));
        IngestResult r = gw.ingestRegister(new RegisterFrame("dev-001", "R02", "60", 2000L));

        assertTrue(j.accepted());
        assertTrue(r.accepted());
        assertEquals("JSON", j.record().source());
        assertEquals("REGISTER", r.record().source());
        assertEquals("humidity", r.record().metric());
        ScenarioLog.summary("归一化", "JSON 与寄存器帧均归一为 TelemetryRecord，寄存器地址已映射指标名");
    }

    @Test
    void softRangeViolationIsDegradedButKept(@TempDir Path dir) {
        GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        IngestResult result = gw.ingestJson(json("dev-001", "temperature", 100, 1000));

        assertTrue(result.accepted());
        assertEquals(Quality.DEGRADED, result.record().quality());
        assertFalse(result.degradeReasons().isEmpty());
        ScenarioLog.step("降级", "temperature=100 (软界[-20,80] 硬界[-40,125])",
                "超可信区间但在硬量程内", "GOOD->DEGRADED，数据保留并继续上联");
    }

    @Test
    void invalidInputsAreRejectedWithDistinctReasons(@TempDir Path dir) {
        GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        assertReject(gw.ingestJson("{not-json"), RejectReason.MALFORMED, "非法报文");
        assertReject(gw.ingestJson(json("dev-999", "temperature", 1, 1)),
                RejectReason.UNKNOWN_DEVICE, "未登记设备");
        assertReject(gw.ingestJson(json("dev-001", "voltage", 1, 2)),
                RejectReason.UNKNOWN_METRIC, "未声明指标");
        assertReject(gw.ingestJson("{\"deviceId\":\"dev-001\",\"metric\":\"temperature\"}"),
                RejectReason.MISSING_FIELD, "缺字段");
        assertReject(gw.ingestJson(json("dev-001", "temperature", "abc", 3)),
                RejectReason.INVALID_VALUE, "非数值");
        assertReject(gw.ingestJson(json("dev-001", "temperature", 500, 4)),
                RejectReason.OUT_OF_RANGE, "超硬量程");
        assertReject(gw.ingestRegister(new RegisterFrame("dev-001", "R99", "1", 5L)),
                RejectReason.UNKNOWN_METRIC, "未知寄存器");

        long distinctReasons = gw.deadLetterLog().all().stream()
                .map(e -> e.reason()).distinct().count();
        assertTrue(distinctReasons >= 6, "拒收原因必须可区分，实际分类数=" + distinctReasons);
        ScenarioLog.summary("错误分类", "全部异常输入进入死信日志，原因枚举可区分: "
                + gw.deadLetterLog().all().stream().map(e -> e.reason().toString()).toList());
    }

    @Test
    void perDeviceOrderAndDuplicateAreStable(@TempDir Path dir) {
        GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        assertTrue(gw.ingestJson(json("dev-001", "temperature", 20, 1000)).accepted());
        assertReject(gw.ingestJson(json("dev-001", "temperature", 21, 900)),
                RejectReason.OUT_OF_ORDER, "迟到时间戳");
        assertReject(gw.ingestJson(json("dev-001", "temperature", 22, 1000)),
                RejectReason.DUPLICATE, "完全重复采样");

        assertTrue(gw.ingestJson(json("dev-002", "temperature", 30, 500)).accepted(),
                "不同设备各自独立水位线，互不影响");

        assertEquals(2, gw.archive().all().size());
        ScenarioLog.summary("顺序与去重",
                "乱序拒收、重复拒收、跨设备水位线独立；同设备接收顺序与时间戳单调一致");
    }

    private void assertReject(IngestResult result, RejectReason expected, String label) {
        assertFalse(result.accepted(), label + " 应被拒收");
        assertEquals(expected, result.reason(), label + " 原因分类错误");
        ScenarioLog.step("拒收-" + expected, label, "命中校验规则 " + expected, "写入 dead-letter.jsonl");
    }
}
