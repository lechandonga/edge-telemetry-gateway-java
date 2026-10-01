package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.ingest.RegisterSample;
import com.github.lechandonga.edgetelemetrygatewayjava.model.IngestResult;
import com.github.lechandonga.edgetelemetrygatewayjava.model.Quality;
import com.github.lechandonga.edgetelemetrygatewayjava.model.RejectReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 两种上报形式归一、错误分类、乱序降级/拒收、同设备顺序稳定。 */
class IngestNormalizationTest {

    private final List<String> logs = new ArrayList<>();

    @Test
    void registerAndJsonNormalizeToSameModel(@TempDir Path dir) {
        TestEnv env = TestEnv.create(dir, 100,
                com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer.OutOfOrderPolicy.CLAMP,
                logs::add);
        IngestResult reg = env.gateway()
                .ingestRegister(new RegisterSample("dev-A", 40001, 21.5, 1000L));
        IngestResult json = env.gateway()
                .ingestJson("{\"deviceId\":\"dev-B\",\"metric\":\"temperature\",\"value\":22.5,\"ts\":1000}");

        assertEquals(IngestResult.Status.ACCEPTED, reg.status());
        assertEquals(IngestResult.Status.ACCEPTED, json.status());
        assertEquals("temperature", reg.record().metric());
        assertEquals("temperature", json.record().metric());
        assertNotNull(reg.record().sourceType());
        assertNotNull(json.record().sourceType());
        // 记录进入缓冲后由缓冲层分配全局 recordId
        assertTrue(env.cloud().received().stream()
                .anyMatch(r -> r.deviceId().equals("dev-A")));
        printLogs();
    }

    @Test
    void invalidInputsGetDistinguishableRejections(@TempDir Path dir) {
        TestEnv env = TestEnv.create(dir, 100,
                com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer.OutOfOrderPolicy.CLAMP,
                logs::add);

        assertEquals(RejectReason.MALFORMED_PAYLOAD,
                env.gateway().ingestJson("{bad").reason());
        assertEquals(RejectReason.UNKNOWN_DEVICE,
                env.gateway().ingestRegister(new RegisterSample("dev-X", 40001, 1.0, 1L)).reason());
        assertEquals(RejectReason.VALUE_OUT_OF_RANGE,
                env.gateway().ingestRegister(new RegisterSample("dev-A", 40001, 999.0, 1L)).reason());
        assertEquals(RejectReason.MISSING_FIELD,
                env.gateway().ingestJson("{\"deviceId\":\"dev-A\",\"metric\":\"temperature\"}").reason());
        assertEquals(RejectReason.MISSING_FIELD,
                env.gateway().ingestRegister(new RegisterSample("dev-A", 40009, 1.0, 1L)).reason());
        assertEquals(0, env.cloud().receivedCount(), "拒收记录不得进入链路");
        printLogs();
    }

    @Test
    void outOfOrderIsDegradedWithClampedTimestampByDefault(@TempDir Path dir) {
        TestEnv env = TestEnv.create(dir, 100,
                com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer.OutOfOrderPolicy.CLAMP,
                logs::add);
        env.gateway().ingestRegister(new RegisterSample("dev-A", 40001, 30.0, 1000L));
        IngestResult late = env.gateway()
                .ingestRegister(new RegisterSample("dev-A", 40001, 31.0, 500L));
        assertEquals(IngestResult.Status.DEGRADED, late.status());
        assertEquals(Quality.TIMESTAMP_CLAMPED, late.record().quality());
        assertEquals(1000L, late.record().timestampMs(), "时间戳钳制为设备最新值");
        // deviceSeq 单调，顺序稳定
        assertEquals(2L, late.record().deviceSeq());
        printLogs();
    }

    @Test
    void outOfOrderCanBeRejectedUnderRejectPolicy(@TempDir Path dir) {
        TestEnv env = TestEnv.create(dir, 100,
                com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer.OutOfOrderPolicy.REJECT,
                logs::add);
        env.gateway().ingestRegister(new RegisterSample("dev-A", 40001, 30.0, 1000L));
        IngestResult late = env.gateway()
                .ingestRegister(new RegisterSample("dev-A", 40001, 31.0, 900L));
        assertEquals(IngestResult.Status.REJECTED, late.status());
        assertEquals(RejectReason.OUT_OF_ORDER, late.reason());
        printLogs();
    }

    private void printLogs() {
        logs.forEach(System.out::println);
        logs.clear();
    }
}
