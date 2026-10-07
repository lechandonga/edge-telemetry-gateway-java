package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.demo;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleSet;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.ThresholdRule;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.UplinkManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * 本地端到端演示：无任何外部服务依赖。
 *
 * 运行：mvn -q exec:java -Dexec.mainClass=...LocalDemo
 * （或在 IDE 中直接运行 main）。可用 -Ddata.dir=/path 指定数据目录，默认 ./data/demo。
 */
public final class LocalDemo {

    private LocalDemo() {
    }

    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of(System.getProperty("data.dir", "data/demo"));
        if (Files.exists(dataDir)) {
            try (var paths = Files.walk(dataDir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception ignored) {
                        // 演示目录清理尽力而为
                    }
                });
            }
        }
        Files.createDirectories(dataDir);

        RuleStore ruleStore = new RuleStore(dataDir);
        ruleStore.publish(new RuleSet("v1", System.currentTimeMillis(), List.of(
                new ThresholdRule("temp-high", "dev-001", "temperature",
                        ThresholdRule.Operator.GT, 30, 5_000))));
        SimulatedCloud cloud = new SimulatedCloud();
        AlertEngine alerts = new AlertEngine(dataDir, ruleStore);
        TelemetryGateway gw = new TelemetryGateway(dataDir, 20,
                DeviceRegistry.defaultRegistry(), cloud, alerts);

        System.out.println("== 1. 正常上报（JSON + 寄存器）==");
        gw.ingestJson("""
                {"deviceId":"dev-001","metric":"temperature","value":22.0,"timestamp":1000}
                """.trim());
        gw.ingestRegister(new RegisterFrame("dev-001", "R01", "23.5", 2_000L));

        System.out.println("== 2. 异常输入：降级 / 拒收 ==");
        gw.ingestJson("""
                {"deviceId":"dev-001","metric":"humidity","value":2,"timestamp":3000}
                """.trim());
        gw.ingestJson("""
                {"deviceId":"dev-999","metric":"temperature","value":1,"timestamp":4000}
                """.trim());
        gw.ingestJson("{broken-json");
        System.out.println("死信条数=" + gw.deadLetterLog().all().size());

        System.out.println("== 3. 断连积压 + ACK 抖动 ==");
        cloud.setAvailable(false);
        gw.ingestJson(sample(35, 6_000));
        gw.ingestJson(sample(36, 8_000));
        System.out.println("断连滞留=" + gw.pendingCount());
        cloud.setAvailable(true);
        cloud.simulateAckLossOnce();
        gw.flushPending();
        System.out.println("ACK 抖动后滞留=" + gw.pendingCount() + "（重发）");
        gw.flushPending();
        System.out.println("补传完成滞留=" + gw.pendingCount() + " 云端接收=" + cloud.acceptedCount());

        System.out.println("== 4. 持续窗口告警与恢复 ==");
        gw.ingestJson(sample(37, 12_000));
        gw.ingestJson(sample(20, 14_000));
        alerts.events().forEach(e -> System.out.println("事件: " + e.type()
                + " rule=" + e.ruleId() + " version=" + e.ruleVersion() + " @ " + e.eventTimestamp()));

        System.out.println("== 5. 批量补传 + 永久拒收隔离 + 对账 ==");
        cloud.setAvailable(false);
        for (int i = 0; i < 10; i++) {
            gw.ingestJson(sample(21, 20_000L + i * 1_000L));
        }
        System.out.println("断连积压=" + gw.pendingCount() + " 批大小="
                + UplinkManager.DEFAULT_BATCH_SIZE + "（硬上限 " + UplinkManager.MAX_BATCH_SIZE + "）");
        // 其中一条云端永久拒收（模拟历史超时限），不应堵住后面的数据。
        cloud.rejectPermanentlyIf(
                r -> r.timestamp() == 23_000L,
                CloudRejectReason.EXPIRED, "历史数据超过云端接收时限");
        cloud.setAvailable(true);
        gw.flushPending();
        System.out.println("批量补传后滞留=" + gw.pendingCount()
                + " 隔离区=" + gw.quarantineStore().size()
                + " 云端批量调用=" + cloud.batchCallCount());
        gw.quarantineStore().all().forEach(e -> System.out.println(
                "隔离: key=" + e.idempotencyKey() + " reason=" + e.reason() + " detail=" + e.detail()));
        System.out.println(gw.reconcile());

        System.out.println("== 6. 规则升级到 v2 ==");
        alerts.publishRules(new RuleSet("v2", System.currentTimeMillis(), List.of(
                new ThresholdRule("temp-high", "dev-001", "temperature",
                        ThresholdRule.Operator.GT, 25, 3_000))));
        var v1 = AlertEngine.replay(gw.archive().all(), ruleStore.version("v1").orElseThrow());
        var v2 = AlertEngine.replay(gw.archive().all(), ruleStore.version("v2").orElseThrow());
        System.out.println("同段历史重放：v1 FIRE 数=" + countFire(v1) + "，v2 FIRE 数=" + countFire(v2));

        System.out.println("数据目录: " + dataDir.toAbsolutePath());
    }

    private static String sample(double value, long ts) {
        return String.format(
                "{\"deviceId\":\"dev-001\",\"metric\":\"temperature\",\"value\":%s,\"timestamp\":%d}",
                value, ts);
    }

    private static long countFire(List<com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEvent> events) {
        return events.stream().filter(e -> "FIRE".equals(e.type())).count();
    }
}
