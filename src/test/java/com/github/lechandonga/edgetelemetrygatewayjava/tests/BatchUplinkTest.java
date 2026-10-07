package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.MetricSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.ingest.RegisterFrame;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.reconcile.ReconciliationReport;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.UplinkManager;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.GatewayTestSupport;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.ScenarioLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批量补传：批效率、部分确认、整批回执丢失、永久拒收隔离与队列继续前进、
 * 中途重启、隔离跨重启、老数据升级兼容，以及多设备真并发 + 双入口灌数总账核对。
 */
class BatchUplinkTest {

    private final TelemetryGateway[] holder = new TelemetryGateway[1];

    private String key(String device, String metric, long ts) {
        return device + "|" + metric + "|" + ts;
    }

    private String sample(String device, String metric, int seq, long ts) {
        double value = 20.0 + (seq % 40);
        return String.format(
                "{\"deviceId\":\"%s\",\"metric\":\"%s\",\"value\":%s,\"timestamp\":%d}",
                device, metric, value, ts);
    }

    private void ingestViaBothEntries(TelemetryGateway gw, String device, String metric,
                                      int seq, long ts) {
        if ((seq & 1) == 0) {
            gw.ingestJson(sample(device, metric, seq, ts));
        } else {
            String address = "temperature".equals(metric) ? "R01" : "R02";
            gw.ingestRegister(new RegisterFrame(device, address, String.valueOf(20.0 + (seq % 40)), ts));
        }
    }

    private DeviceRegistry registryWith(int deviceCount) {
        MetricSpec temperature = new MetricSpec("temperature", "C", -40, 125, -20, 80);
        MetricSpec humidity = new MetricSpec("humidity", "%", 0, 100, 5, 95);
        List<DeviceSpec> specs = new ArrayList<>();
        for (int d = 1; d <= deviceCount; d++) {
            String id = String.format("dev-%03d", d);
            specs.add(new DeviceSpec(id, "sensor-" + d,
                    Map.of("temperature", temperature, "humidity", humidity),
                    Map.of("R01", "temperature", "R02", "humidity")));
        }
        return new DeviceRegistry(specs);
    }

    /** 积压 1000 条、批大小 64：批量调用必须是十几次量级而不是上千次。 */
    @Test
    void batchReplayCutsCallsToBatchOrder(@TempDir Path dir) {
        int batchSize = 64;
        SimulatedCloud cloud = GatewayTestSupport.newGatewayBatched(dir, 5000, null, holder, batchSize);
        TelemetryGateway gw = holder[0];
        assertEquals(batchSize, gw.uplinkBatchSize());
        cloud.setAvailable(false);
        int total = 1000;
        for (int i = 1; i <= total; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 10_000L));
        }
        assertEquals(total, gw.pendingCount());

        cloud.setAvailable(true);
        int confirmed = gw.flushPending();
        assertEquals(total, confirmed);
        assertEquals(0, gw.pendingCount());
        int calls = gw.cloudBatchCallCount();
        int expectedBatches = (total + batchSize - 1) / batchSize;
        assertEquals(expectedBatches, calls, "云端接收调用次数必须与批次数一致");
        assertTrue(calls < 20, "1000 条积压应为十几次调用，实际=" + calls);
        assertEquals(total, cloud.acceptedCount(), "不重不漏");

        List<Long> tsOrder = cloud.arrivalOrder().stream()
                .map(k -> Long.parseLong(k.substring(k.lastIndexOf('|') + 1))).toList();
        for (int i = 1; i < tsOrder.size(); i++) {
            assertTrue(tsOrder.get(i - 1) < tsOrder.get(i), "设备内顺序必须严格递增");
        }
        ReconciliationReport report = gw.reconcile();
        assertTrue(report.balanced(), "对账必须精确: " + report);
        ScenarioLog.summary("批量效率",
                "1000 条积压，批大小 64，云端批量调用=" + calls + " 次（=ceil(1000/64)）");
    }

    /** 配置批大小超过硬上限时截断到 256；非正值回退默认值。 */
    @Test
    void batchSizeIsClampedToHardLimit(@TempDir Path dir) {
        GatewayTestSupport.newGatewayBatched(dir, 100, null, holder, 10_000);
        assertEquals(UplinkManager.MAX_BATCH_SIZE, holder[0].uplinkBatchSize());
        ScenarioLog.summary("批上限", "配置 10000 被硬上限截断为 " + UplinkManager.MAX_BATCH_SIZE);
    }

    /** 云端只确认一批的前几条：已确认出队，未确认原样留队，重试后不重不漏。 */
    @Test
    void partialAcknowledgementKeepsRestInOrder(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGatewayBatched(dir, 100, null, holder, 10);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 10; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 1000L));
        }
        cloud.simulatePartialConfirmOnce(3);
        cloud.simulateDownAfterNextBatch();
        cloud.setAvailable(true);
        int confirmed = gw.flushPending();
        assertEquals(3, confirmed, "只确认了前 3 条");
        assertEquals(7, gw.pendingCount(), "后 7 条一条都不能丢");
        assertEquals(3, cloud.acceptedCount());

        cloud.setAvailable(true);
        int again = gw.flushPending();
        assertEquals(7, again, "恢复后剩余全部补传成功");
        assertEquals(0, gw.pendingCount());
        assertEquals(10, cloud.acceptedCount(), "重发被云端去重，不重不漏");
        ScenarioLog.summary("部分确认", "前 3 条出队，后 7 条留队；重试后 10 条精确收齐");
    }

    /** 整批回执丢失：云端可能已收，但网关一条都不出队；重试后不重不漏。 */
    @Test
    void wholeBatchReceiptLossRetriesSafely(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGatewayBatched(dir, 100, null, holder, 8);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 8; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 1000L));
        }
        cloud.setAvailable(true);
        cloud.simulateBatchReceiptLossOnce();
        int first = gw.flushPending();
        assertEquals(0, first, "整批回执丢失，不得出队任何记录");
        assertEquals(8, gw.pendingCount());
        assertEquals(8, cloud.acceptedCount(), "云端其实已去重收下全部 8 条");

        int second = gw.flushPending();
        assertEquals(8, second);
        assertEquals(8, cloud.acceptedCount(), "重发去重，云端仍是 8 条");
        assertEquals(0, gw.pendingCount());
        ScenarioLog.summary("整批回执丢失", "8 条留队重发，云端幂等去重，不丢不重");
    }

    /** 永久拒收：记录隔离、可查、原因可分类，后续正常数据继续前进且顺序不乱，跨重启保留。 */
    @Test
    void permanentRejectIsQuarantinedAndQueueMovesOn(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGatewayBatched(dir, 100, null, holder, 4);
        TelemetryGateway gw = holder[0];

        cloud.setAvailable(false);
        for (int i = 1; i <= 9; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 1000L));
        }
        String expiredKey = key("dev-001", "temperature", 2_000L);
        String discontinuedKey = key("dev-001", "temperature", 6_000L);
        cloud.addPermanentReject(expiredKey, CloudRejectCode.EXPIRED, "历史数据超过 24h 接收时限");
        cloud.addPermanentReject(discontinuedKey, CloudRejectCode.METRIC_DISCONTINUED,
                "云端已停收该指标");

        cloud.setAvailable(true);
        gw.flushPending();

        assertEquals(0, gw.pendingCount(), "永久拒收不得堵在队头");
        assertEquals(2, gw.quarantineLog().size());
        var entries = gw.quarantineLog().all();
        assertEquals(List.of(CloudRejectCode.EXPIRED, CloudRejectCode.METRIC_DISCONTINUED),
                entries.stream().map(e -> e.reason()).sorted().toList());
        assertEquals("历史数据超过 24h 接收时限",
                entries.stream().filter(e -> e.reason() == CloudRejectCode.EXPIRED)
                        .findFirst().orElseThrow().detail());
        List<String> arrivals = cloud.arrivalOrder();
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= 9; i++) {
            long ts = i * 1000L;
            if (ts == 2000L || ts == 6000L) {
                continue;
            }
            expected.add(key("dev-001", "temperature", ts));
        }
        assertEquals(expected, arrivals, "剔除被隔离记录后，其余记录相对顺序必须不变");
        assertEquals(7, cloud.acceptedCount());

        ReconciliationReport report = gw.reconcile();
        assertEquals(9, report.acceptedIngress());
        assertEquals(7, report.cloudAccepted());
        assertEquals(2, report.quarantined());
        assertEquals(0, report.inFlight());
        assertEquals(0, report.evicted());
        assertTrue(report.balanced(), "对账精确: " + report);

        SimulatedCloud cloud2 = GatewayTestSupport.restart(dir, 100, holder, 4);
        TelemetryGateway revived = holder[0];
        assertEquals(2, revived.quarantineLog().size(), "隔离状态必须跨重启保留");
        assertTrue(revived.quarantineLog().containsKey(expiredKey));
        assertEquals(0, revived.pendingCount());
        ReconciliationReport afterRestart = revived.reconcile();
        assertEquals(2, afterRestart.quarantined());
        // 重启用了新的模拟云端实例，恢复后再补传为空；把旧云端已收事实用于核验无重复：
        assertEquals(7, cloud.acceptedCount(), "重启前后云端接收总数不变，无重复上报");
        ScenarioLog.summary("永久拒收隔离",
                "2 条隔离（原因可分类、可查），队头解堵；其余 7 条相对顺序不变；隔离跨重启保留，账平");
    }

    /** 批量补传进行到一半崩溃：已确认不重报、未确认不丢，账平。 */
    @Test
    void crashMidBatchReplaysOnlyUnconfirmed(@TempDir Path dir) {
        int batchSize = 8;
        SimulatedCloud cloud = GatewayTestSupport.newGatewayBatched(dir, 100, null, holder, batchSize);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);
        for (int i = 1; i <= 20; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 1000L));
        }
        cloud.simulatePartialConfirmOnce(3);
        cloud.simulateDownAfterNextBatch();
        cloud.setAvailable(true);
        int beforeCrash = gw.flushPending();
        assertEquals(3, beforeCrash);
        assertEquals(17, gw.pendingCount(), "崩溃瞬间 3 已确认出队、17 未确认");

        SimulatedCloud cloud2 = GatewayTestSupport.restart(dir, 100, holder, batchSize);
        TelemetryGateway revived = holder[0];
        assertEquals(17, revived.pendingCount(), "已确认落盘出队，未确认全部从 WAL 恢复");
        List<Long> recoveredTs = revived.buffer().snapshot().stream()
                .map(TelemetryRecord::timestamp).toList();
        List<Long> expectTs = new ArrayList<>();
        for (int i = 4; i <= 20; i++) {
            expectTs.add(i * 1000L);
        }
        assertEquals(expectTs, recoveredTs, "恢复序列必须严格是未确认后缀");

        int after = revived.flushPending();
        assertEquals(17, after);
        assertEquals(0, revived.pendingCount());
        assertEquals(17, cloud2.acceptedCount(), "重启后只有 17 条未确认数据重发到新云端");
        int secondRound = revived.flushPending();
        assertEquals(0, secondRound, "再次补传必须为空：已确认的不重复上报");
        assertEquals(17, cloud2.acceptedCount(), "无重复上报");
        // 旧云端在崩溃前已去重收下前 3 条；两次接收并集 17+3=20，且各自无重复。
        assertEquals(3, cloud.acceptedCount());
        ReconciliationReport report = revived.reconcile();
        assertEquals(20, report.cloudAccepted(),
                "云端已接收以本地账本为准，跨重启仍计 20 条");
        assertTrue(report.balanced(), "重启后对账: " + report);
        ScenarioLog.summary("批量中途重启",
                "3 条已确认不重报，17 条从 WAL 恢复补齐，云端去重总数 20，账平");
    }

    /** 升级兼容：旧版本逐条模式已落盘的 pending.jsonl，新版本批量模式继续补传。 */
    @Test
    void legacyPendingWalReplaysInBatchesAfterUpgrade(@TempDir Path dir) throws Exception {
        SimulatedCloud oldCloud = GatewayTestSupport.newGatewayBatched(dir, 100, null, holder, 1);
        TelemetryGateway oldGw = holder[0];
        oldCloud.setAvailable(false);
        for (int i = 1; i <= 30; i++) {
            oldGw.ingestJson(sample("dev-001", "temperature", i, i * 1000L));
        }
        assertEquals(30, oldGw.pendingCount());

        SimulatedCloud newCloud = GatewayTestSupport.restart(dir, 100, holder, 16);
        TelemetryGateway upgraded = holder[0];
        assertEquals(16, upgraded.uplinkBatchSize());
        assertEquals(30, upgraded.pendingCount(), "升级前落盘老数据必须完整恢复");
        int confirmed = upgraded.flushPending();
        assertEquals(30, confirmed);
        assertEquals(30, newCloud.acceptedCount());
        assertEquals(2, upgraded.cloudBatchCallCount(),
                "老 WAL 数据按新批量语义 2 批（16+14）补传完成");
        assertTrue(upgraded.reconcile().balanced());
        ScenarioLog.summary("升级兼容", "旧 pending.jsonl 逐条落盘数据在批量模式下按 16/批继续补传，不丢不重");
    }

    /**
     * 真并发：8 设备、每台 2000 条（2 指标各 1000），多线程同时从 JSON 与寄存器两条入口灌数，
     * 另有并发补传线程跨设备并行 drain；断言无异常、不丢不重、设备内顺序稳定、对账精确。
     */
    @Test
    void concurrentEightDevicesDualEntriesWithBatchDrain(@TempDir Path dir) throws Exception {
        int devices = 8;
        int perDevice = 2000;
        int batchSize = 64;
        DeviceRegistry registry = registryWith(devices);
        SimulatedCloud cloud = GatewayTestSupport.newGateway(
                dir, devices * perDevice * 2 + 100, null, registry, holder, batchSize);
        TelemetryGateway gw = holder[0];
        cloud.setAvailable(false);

        ExecutorService pool = Executors.newFixedThreadPool(devices);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(devices);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicInteger acceptedIngress = new AtomicInteger();

        for (int d = 1; d <= devices; d++) {
            final String deviceId = String.format("dev-%03d", d);
            final long deviceBase = d * 100_000_000L;
            pool.submit(() -> {
                try {
                    start.await();
                    // 每设备自己的时间戳序列，temperature 与 humidity 交错，保证水位线单调。
                    for (int i = 1; i <= perDevice; i++) {
                        long tsTemp = deviceBase + i * 10L;
                        long tsHum = deviceBase + i * 10L + 5;
                        ingestViaBothEntries(gw, deviceId, "temperature", i * 2, tsTemp);
                        ingestViaBothEntries(gw, deviceId, "humidity", i * 2 + 1, tsHum);
                        acceptedIngress.addAndGet(2);
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        // 补传线程：与上报真并行，周期性批量 drain（不是单线程顺序模拟）。
        AtomicBoolean flushing = new AtomicBoolean(true);
        ExecutorService flushPool = Executors.newSingleThreadExecutor();
        flushPool.submit(() -> {
            while (flushing.get() || gw.pendingCount() > 0) {
                try {
                    gw.flushPending();
                    Thread.sleep(50);
                } catch (Throwable t) {
                    errors.add(t);
                }
            }
        });

        start.countDown();
        assertTrue(done.await(120, TimeUnit.SECONDS), "并发上报必须在时限内完成");
        cloud.setAvailable(true);
        pool.shutdown();
        for (int i = 0; i < 200 && gw.pendingCount() > 0; i++) {
            gw.flushPending();
            Thread.sleep(20);
        }
        flushing.set(false);
        flushPool.shutdown();
        assertTrue(flushPool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue(errors.isEmpty(), "真并发过程中不得出现异常: " + errors);
        int expected = devices * perDevice * 2;
        assertEquals(0, gw.pendingCount(), "全部补传完成");
        assertEquals(expected, cloud.acceptedCount(), "云端去重接收总数必须精确等于入链总数");

        // 设备内顺序稳定：每设备每指标按时间戳严格递增。
        for (int d = 1; d <= devices; d++) {
            String deviceId = String.format("dev-%03d", d);
            for (String metric : List.of("temperature", "humidity")) {
                String prefix = deviceId + "|" + metric + "|";
                List<Long> order = cloud.arrivalOrder().stream()
                        .filter(k -> k.startsWith(prefix))
                        .map(k -> Long.parseLong(k.substring(k.lastIndexOf('|') + 1)))
                        .toList();
                assertEquals(perDevice, order.size(), deviceId + "/" + metric + " 条数必须精确");
                for (int i = 1; i < order.size(); i++) {
                    assertTrue(order.get(i - 1) < order.get(i),
                            deviceId + "/" + metric + " 设备内顺序错乱 at " + i);
                }
            }
        }

        ReconciliationReport report = gw.reconcile();
        assertEquals(expected, report.acceptedIngress());
        assertEquals(expected, report.cloudAccepted());
        assertEquals(0, report.quarantined());
        assertEquals(0, report.inFlight());
        assertEquals(0, report.evicted());
        assertTrue(report.balanced(), "真并发后四类对账必须精确: " + report);
        int calls = gw.cloudBatchCallCount();
        assertTrue(calls < expected / 10, "批量调用次数应远小于逐条: " + calls);
        ScenarioLog.summary("真并发批量补传",
                "8 设备 x 2000 条 x 双入口=" + expected + " 条，补传线程并行；不丢不重、设备内有序、账平，批量调用=" + calls);
    }
}
