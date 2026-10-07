package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.demo;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleSet;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.ThresholdRule;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;

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
        int batchSize = Integer.getInteger("uplink.batchSize", 16);
        TelemetryGateway gw = new TelemetryGateway(dataDir, 2000,
                DeviceRegistry.defaultRegistry(), cloud, alerts, batchSize);

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

        System.out.println("== 3. 断连积压 + 批量补传（批大小=" + batchSize + "） ==");
        cloud.setAvailable(false);
        // 云端对一条超龄历史数据永久拒收（EXPIRED）：只隔离它，不堵队头。
        cloud.addPermanentReject("dev-001|temperature|7000",
                CloudRejectCode.EXPIRED, "历史数据超过云端接收时限");
        for (int i = 1; i <= 100; i++) {
            gw.ingestJson(sample(20 + (i % 10), 6_000L + i * 1_000L));
        }
        System.out.println("断连滞留=" + gw.pendingCount());
        cloud.setAvailable(true);
        gw.flushPending();
        System.out.println("批量补传完成 滞留=" + gw.pendingCount()
                + " 云端接收=" + cloud.acceptedCount()
                + " 隔离=" + gw.quarantineLog().size()
                + " 云端批量调用次数=" + gw.cloudBatchCallCount());
        var report = gw.reconcile();
        System.out.printf("对账: 入链=%d 云端已接收=%d 已隔离=%d 在途=%d 已淘汰=%d 平衡=%s%n",
                report.acceptedIngress(), report.cloudAccepted(), report.quarantined(),
                report.inFlight(), report.evicted(), report.balanced());
        gw.quarantineLog().all().forEach(e -> System.out.println(
                "隔离记录: key=" + e.idempotencyKey() + " reason=" + e.reason()
                        + " detail=" + e.detail()));

        System.out.println("== 4. 持续窗口告警与恢复 ==");
        gw.ingestJson(sample(37, 12_000));
        gw.ingestJson(sample(20, 14_000));
        alerts.events().forEach(e -> System.out.println("事件: " + e.type()
                + " rule=" + e.ruleId() + " version=" + e.ruleVersion() + " @ " + e.eventTimestamp()));

        System.out.println("== 5. 规则升级到 v2 ==");
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
