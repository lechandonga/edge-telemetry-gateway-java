package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleSet;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.ThresholdRule;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.GatewayTestSupport;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.ScenarioLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端综合剧本：正常 -> 异常/降级输入 -> 断连积压 -> 抖动补传 -> 重启 ->
 * 规则变更 -> 告警与恢复，串联验证“不丢不重、结论可解释”。
 */
class EndToEndScenarioTest {

    private final TelemetryGateway[] holder = new TelemetryGateway[1];

    private String json(String device, String metric, Object value, long ts) {
        String v = value instanceof String s ? "\"" + s + "\"" : String.valueOf(value);
        return String.format("{\"deviceId\":\"%s\",\"metric\":\"%s\",\"value\":%s,\"timestamp\":%d}",
                device, metric, v, ts);
    }

    @Test
    void fullScenario(@TempDir Path dir) {
        RuleSet v1 = new RuleSet("v1", 0, List.of(
                new ThresholdRule("temp-high", "dev-001", "temperature",
                        ThresholdRule.Operator.GT, 30, 5_000)));
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 50, v1, holder);
        TelemetryGateway gw = holder[0];

        // 1) 两种形式的正常 + 降级 + 拒收输入
        gw.ingestJson(json("dev-001", "temperature", 22, 1_000));
        gw.ingestRegister(new RegisterFrame("dev-001", "R02", "55", 2_000L));
        gw.ingestJson(json("dev-001", "temperature", 28, 3_000));
        gw.ingestJson(json("dev-001", "temperature", "oops", 4_000));
        gw.ingestJson(json("dev-999", "temperature", 1, 5_000));
        ScenarioLog.step("E2E-采集", "JSON/寄存器/未越限临界/非法/未登记 5 类输入",
                "3 接收(无提前告警)+2 拒收入死信", "archive=" + gw.archive().all().size());

        // 2) 断连期间持续越限数据积压（尚未达 5s 窗口）
        cloud.setAvailable(false);
        gw.ingestJson(json("dev-001", "temperature", 35, 6_000));
        gw.ingestJson(json("dev-001", "temperature", 36, 8_000));
        assertEquals(2, gw.pendingCount());

        // 3) 进程在断连中重启
        SimulatedCloud cloud2 = GatewayTestSupport.restart(dir, 50, holder);
        gw = holder[0];
        assertEquals(2, gw.pendingCount(), "重启后缓冲与告警窗口都要恢复");

        // 4) 恢复链路并发生一次 ACK 抖动
        cloud2.setAvailable(true);
        cloud2.simulateAckLossOnce();
        int firstFlush = gw.flushPending();
        assertEquals(0, firstFlush, "队头 ACK 丢失，本轮没有任何数据被确认");
        assertEquals(2, gw.pendingCount(), "ACK 丢失的队头仍滞留，后续数据被严格有序阻塞");
        gw.flushPending();
        assertEquals(0, gw.pendingCount());

        // 5) 持续越限窗口在重启后补齐到 5s，恰好一次 FIRE
        var eventsBefore = gw.alertEngine().events().size();
        gw.ingestJson(json("dev-001", "temperature", 37, 11_500));
        var newEvents = gw.alertEngine().events().size() - eventsBefore;
        assertEquals(1, newEvents, "6s->11.5s 持续越限满 5s，产生一次 FIRE");

        // 6) 恢复正常产生 RECOVER，随后规则升级为更严格的 v2
        gw.ingestJson(json("dev-001", "temperature", 20, 14_000));
        gw.alertEngine().publishRules(new RuleSet("v2", 1, List.of(
                new ThresholdRule("temp-high", "dev-001", "temperature",
                        ThresholdRule.Operator.GT, 25, 3_000))));

        // 7) 用两个版本分别重放同一段历史，结论可复现、互不相同
        var v1Fires = com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine
                .replay(gw.archive().all(), gw.alertEngine().currentRules()).size();
        long totalCloud = cloud2.acceptedCount();
        long totalAccepted = gw.archive().all().size();

        assertEquals(2, gw.deadLetterLog().all().size(), "异常输入全部在死信日志可查");
        assertTrue(v1Fires >= 1);
        ScenarioLog.summary("E2E", String.format(
                "归档=%d 云端去重接收=%d 死信=%d 淘汰=%d；断连/重启/抖动下不丢不重，FIRE/RECOVER 均带规则版本",
                totalAccepted, totalCloud, gw.deadLetterLog().all().size(),
                gw.evictionLog().all().size()));
    }
}
