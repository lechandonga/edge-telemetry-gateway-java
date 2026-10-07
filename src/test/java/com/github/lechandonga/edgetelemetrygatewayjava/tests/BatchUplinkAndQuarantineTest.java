package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.MetricSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectReason;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.storage.QuarantineEntry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.ReconciliationReport;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.GatewayTestSupport;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.ScenarioLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批量补传与永久拒收隔离：部分确认、整批回执丢失、永久拒收不堵队、
 * 批量中途重启、隔离跨重启、调用次数降到批次数量级，以及 8 设备 x 2000+ 条
 * 两条入口真并发灌数据 + 跨设备并行批量补传下的精确对账。
 */
class BatchUplinkAndQuarantineTest {

    private final TelemetryGateway[] holder = new TelemetryGateway[1];

    private static String json(String device, String metric, double value, long ts) {
        return String.format("{\"deviceId\":\"%s\",\"metric\":\"%s\",\"value\":%s,\"timestamp\":%d}",
                device, metric, value, ts);
    }

    private static DeviceRegistry registry(int devices) {
        MetricSpec m = new MetricSpec("m", "u", -1_000_000, 1_000_000, -100, 100);
        List<DeviceSpec> specs = new ArrayList<>();
        for (int d = 1; d <= devices; d++) {
            String id = String.format("dev-%03d", d);
            specs.add(new DeviceSpec(id, "dev" + d,
                    Map.of("m", m, "r", m),
                    Map.of("R01", "r")));
        }
        return new DeviceRegistry(specs);
    }

    @Test
    void batchCallsAreOnOrderOfBatchCount(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 5_000, null,
                registry(2), 64, holder);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 1000; i++) {
            gw.ingestJson(json("dev-001", "m", i, i * 10L));
        }
        assertEquals(1000, gw.pendingCount());

        cloud.setAvailable(true);
        int resolved = gw.flushPending();
        assertEquals(1000, resolved);
        assertEquals(0, gw.pendingCount());
        int calls = cloud.batchCallCount();
        assertEquals(16, calls, "1000 条 / 批大小 64 应为 16 次批量调用，而不是上千次单条调用");
        assertEquals(1000, cloud.acceptedCount());
        ScenarioLog.summary("批量效率",
                "积压 1000 条、批大小 64：批量调用 " + calls + " 次完成全部确认（十几次而非上千次）");
    }

    @Test
    void partialBatchConfirmationKeepsRestInOrder(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 1_000, null,
                registry(2), 10, holder);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 25; i++) {
            gw.ingestJson(json("dev-001", "m", i, i * 10L));
        }

        cloud.setAvailable(true);
        cloud.setBatchConfirmLimit(3);
        int first = gw.flushPending();
        assertEquals(3, first, "云端只确认前 3 条，仅这 3 条出队");
        assertEquals(22, gw.pendingCount());

        cloud.setBatchConfirmLimit(Integer.MAX_VALUE);
        int second = gw.flushPending();
        assertEquals(22, second);
        assertEquals(0, gw.pendingCount());
        assertEquals(25, cloud.acceptedCount(), "未确认部分重发后不丢不重");

        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            expected.add("dev-001|m|" + (i * 10L));
        }
        List<String> dedupInOrder = cloud.arrivalOrder().stream().distinct().toList();
        assertEquals(expected, dedupInOrder, "重发不改变设备内顺序，不产生插队");
        ScenarioLog.summary("部分确认",
                "批次只回执前 3 条 -> 仅前 3 条出队，其余保持原序重发，不丢不重不插队");
    }

    @Test
    void wholeBatchReceiptLossIsRetriedIdempotently(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 1_000, null,
                registry(2), 10, holder);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 25; i++) {
            gw.ingestJson(json("dev-001", "m", i, i * 10L));
        }

        cloud.setAvailable(true);
        cloud.simulateBatchReceiptLossOnce();
        int first = gw.flushPending();
        assertEquals(0, first, "整批回执丢失：一条都不许出队");
        assertEquals(25, gw.pendingCount(), "25 条全部留队");
        assertEquals(10, cloud.observedCount(), "云端实际已收下整批 10 条（幂等去重依据）");

        int second = gw.flushPending();
        assertEquals(25, second);
        assertEquals(0, gw.pendingCount());
        assertEquals(25, cloud.acceptedCount(), "重发被云端幂等去重，不产生重复");
        ScenarioLog.summary("整批回执丢失",
                "云端收下但回执全丢 -> 整批留队重发 -> 云端去重接收仍为 25");
    }

    @Test
    void permanentRejectIsQuarantinedAndQueueAdvances(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 1_000, null,
                registry(2), 10, holder);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        gw.ingestJson(json("dev-001", "m", 1, 1_000L));
        gw.ingestJson(json("dev-001", "m", 2, 2_000L));
        gw.ingestJson(json("dev-001", "m", 3, 3_000L));
        gw.ingestJson(json("dev-001", "m", 4, 4_000L));

        cloud.rejectPermanentlyIf(
                r -> "dev-001".equals(r.deviceId()) && r.timestamp() == 2_000L,
                CloudRejectReason.EXPIRED, "历史数据超过 7 天接收时限");
        cloud.rejectPermanentlyIf(
                r -> "dev-001".equals(r.deviceId()) && r.timestamp() == 3_000L,
                CloudRejectReason.METRIC_DISCONTINUED, "指标 m 已下线");
        cloud.setAvailable(true);
        int resolved = gw.flushPending();
        assertEquals(4, resolved, "2 条确认 + 2 条隔离，都拿到最终结论");
        assertEquals(0, gw.pendingCount(), "永久拒收不堵队头，队列继续前进到空");

        List<QuarantineEntry> q = gw.quarantineStore().all();
        assertEquals(2, q.size());
        assertEquals(CloudRejectReason.EXPIRED, q.get(0).reason());
        assertEquals(CloudRejectReason.METRIC_DISCONTINUED, q.get(1).reason());
        assertEquals(1, gw.quarantineStore().byReason(CloudRejectReason.EXPIRED).size());
        assertEquals(2_000L, q.get(0).record().timestamp());
        assertEquals(3_000L, q.get(1).record().timestamp());

        List<String> acceptedKeys = cloud.arrivalOrder();
        assertEquals(List.of("dev-001|m|1000", "dev-001|m|4000"), acceptedKeys,
                "中间记录被隔离移走，其前后数据相对顺序不变，且被隔离数据不发给云端");

        ReconciliationReport report = gw.reconcile();
        assertTrue(report.balanced());
        assertEquals(4, report.acceptedIntoChain());
        assertEquals(2, report.cloudAccepted());
        assertEquals(2, report.quarantined());
        assertEquals(0, report.inFlight());
        assertEquals(0, report.evicted());

        gw.flushPending();
        assertEquals(2, gw.quarantineStore().size(), "重复永久拒收不产生第二条隔离记录");
        ScenarioLog.summary("永久拒收隔离",
                "2 条永久拒收隔离（EXPIRED / METRIC_DISCONTINUED 可区分、可查询），"
                        + "队列越过它们继续按序补传，前后数据顺序稳定，对账精确配平");
    }

    @Test
    void restartMidBatchDoesNotReconfirmOrLose(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 1_000, null,
                registry(2), 5, holder);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 12; i++) {
            gw.ingestJson(json("dev-001", "m", i, i * 100L));
        }

        cloud.setAvailable(true);
        cloud.setBatchConfirmLimit(2);
        gw.flushPending();
        gw.flushPending();
        assertEquals(8, gw.pendingCount(), "已确认 4 条出队，8 条未确认");

        SimulatedCloud cloud2 = GatewayTestSupport.restart(dir, 1_000, registry(2), 5,
                cloud, holder);
        TelemetryGateway revived = holder[0];
        assertEquals(8, revived.pendingCount(), "未确认数据从 WAL 完整恢复，已确认的不回来");
        cloud2.setAvailable(true);
        cloud2.setBatchConfirmLimit(Integer.MAX_VALUE);
        int resolved = revived.flushPending();
        assertEquals(8, resolved);
        assertEquals(0, revived.pendingCount());
        assertEquals(12, cloud2.acceptedCount(), "重启后云端按幂等去重，总共 12 条不重不漏");
        ScenarioLog.summary("批量中途重启",
                "批次部分确认后崩溃：已确认不重复上报，未确认 8 条从 WAL 恢复并补传完成");
    }

    @Test
    void quarantineSurvivesRestartAndClearsOrphanHead(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 1_000, null,
                registry(2), 5, holder);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        gw.ingestJson(json("dev-001", "m", 1, 1_000L));
        gw.ingestJson(json("dev-001", "m", 2, 2_000L));
        gw.ingestJson(json("dev-001", "m", 3, 3_000L));

        cloud.setAvailable(true);
        cloud.setBatchConfirmLimit(2);
        cloud.rejectPermanentlyIf(
                r -> "dev-001".equals(r.deviceId()) && r.timestamp() == 2_000L,
                CloudRejectReason.EXPIRED, "超时限");
        int first = gw.flushPending();
        assertEquals(2, first, "前 2 条拿到结论：1 接收 + 1 隔离");
        assertEquals(1, gw.quarantineStore().size());

        SimulatedCloud cloud2 = GatewayTestSupport.restart(dir, 1_000, registry(2), 5,
                cloud, holder);
        TelemetryGateway revived = holder[0];
        assertEquals(1, revived.quarantineStore().size(), "隔离状态跨重启仍在、可查询");
        assertEquals(CloudRejectReason.EXPIRED,
                revived.quarantineStore().all().get(0).reason());
        assertEquals(1, revived.pendingCount(),
                "已隔离的队头在恢复时从缓冲清除，只剩最后 1 条未确认数据");

        cloud2.setAvailable(true);
        revived.flushPending();
        assertEquals(0, revived.pendingCount());
        ReconciliationReport report = revived.reconcile();
        assertTrue(report.balanced(), report.toString());
        assertEquals(3, report.acceptedIntoChain());
        assertEquals(2, report.cloudAccepted());
        assertEquals(1, report.quarantined());
        assertEquals(0, report.inFlight());
        assertEquals(0, report.evicted());
        ScenarioLog.summary("隔离跨重启",
                "已隔离记录跨重启可查询；崩溃窗口内的缓冲孤儿队头恢复时安全清除，不重发、不丢失");
    }

    @Test
    void concurrentEightDevicesTwoIngressLanesAndBatchDrain(@TempDir Path dir) throws Exception {
        int devices = 8;
        int perDevicePerLane = 2000;
        int capacity = devices * perDevicePerLane * 2 + 100;
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, capacity, null,
                registry(devices), 64, holder);
        TelemetryGateway gw = holder[0];

        // 云端始终在线：一边多线程从两条既有入口灌数据，一边多线程触发批量补传，
        // 跨设备真正并行（非单线程顺序调用模拟）。
        ExecutorService pool = Executors.newFixedThreadPool(18);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(devices * 2);
        List<Throwable> errors = new ArrayList<>();

        for (int d = 1; d <= devices; d++) {
            final String device = String.format("dev-%03d", d);
            final long jsonBase = d * 100_000_000L;
            final long registerBase = d * 100_000_000L + 50_000_000L;
            // JSON 入口（指标 m，时间戳在该设备区间单调递增）
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 1; i <= perDevicePerLane; i++) {
                        long ts = jsonBase + i * 10L;
                        gw.ingestJson(json(device, "m", i % 100, ts));
                    }
                } catch (Throwable t) {
                    synchronized (errors) {
                        errors.add(t);
                    }
                } finally {
                    done.countDown();
                }
            });
            // 寄存器入口（指标 r，独立水位线区间）
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 1; i <= perDevicePerLane; i++) {
                        long ts = registerBase + i * 10L;
                        gw.ingestRegister(new RegisterFrame(device, "R01",
                                String.valueOf(i % 100), ts));
                    }
                } catch (Throwable t) {
                    synchronized (errors) {
                        errors.add(t);
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        AtomicInteger drainersFinished = new AtomicInteger();
        for (int t = 0; t < 2; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (done.getCount() > 0) {
                        gw.flushPending();
                        Thread.sleep(2);
                    }
                } catch (Throwable e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                } finally {
                    drainersFinished.incrementAndGet();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(120, TimeUnit.SECONDS), "灌数据任务必须全部完成");
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "真并发场景不应出现异常: " + errors);

        int remaining;
        int rounds = 0;
        do {
            remaining = gw.flushPending();
        } while (remaining > 0 && ++rounds < 100);
        assertEquals(0, gw.pendingCount(), "最终全部拿到结论");

        int total = devices * 2 * perDevicePerLane;
        assertEquals(total, gw.archive().all().size(), "入链总数精确");
        ReconciliationReport report = gw.reconcile();
        assertTrue(report.balanced(), report.toString());
        assertEquals(total, report.acceptedIntoChain());
        assertEquals(total, cloud.acceptedCount(), "云端去重接收总数必须等于入链总数（不丢不重）");
        assertEquals(total, report.cloudAccepted());
        assertEquals(0, report.quarantined());
        assertEquals(0, report.inFlight());
        assertEquals(0, report.evicted());
        assertTrue(cloud.batchCallCount() > 0);

        // 设备内顺序稳定：每设备每指标云端首次到达顺序严格按时间戳递增。
        for (int d = 1; d <= devices; d++) {
            String device = String.format("dev-%03d", d);
            for (String metric : List.of("m", "r")) {
                String prefix = device + "|" + metric + "|";
                List<String> keys = cloud.arrivalOrder().stream()
                        .filter(k -> k.startsWith(prefix)).distinct().toList();
                assertEquals(perDevicePerLane, keys.size(),
                        device + "/" + metric + " 不丢不重");
                long lastTs = Long.MIN_VALUE;
                for (String key : keys) {
                    long ts = Long.parseLong(key.substring(key.lastIndexOf('|') + 1));
                    assertTrue(ts > lastTs, device + "/" + metric + " 设备内顺序必须稳定递增");
                    lastTs = ts;
                }
            }
        }
        ScenarioLog.summary("真并发批量补传",
                String.format("8 设备 x 2 入口 x %d 条 = %d 条：多线程并发灌入 + 并发批量补传，"
                                + "无异常、不丢不重、设备内顺序稳定、对账精确配平，批量调用=%d 次",
                        perDevicePerLane, total, cloud.batchCallCount()));
    }
}
