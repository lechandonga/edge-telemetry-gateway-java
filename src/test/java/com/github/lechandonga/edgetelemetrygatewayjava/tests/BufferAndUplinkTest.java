package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.GatewayTestSupport;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.ScenarioLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 断连缓冲、有序补传、ACK 抖动幂等、写满淘汰、重启恢复、多设备并发不丢不重。
 */
class BufferAndUplinkTest {

    private final TelemetryGateway[] holder = new TelemetryGateway[1];

    private String sample(String device, String metric, double value, long ts) {
        return String.format("{\"deviceId\":\"%s\",\"metric\":\"%s\",\"value\":%s,\"timestamp\":%d}",
                device, metric, value, ts);
    }

    @Test
    void outageBuffersThenReplaysInOrder(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        assertTrue(gw.ingestJson(sample("dev-001", "temperature", 1, 1)).accepted());
        assertEquals(0, gw.pendingCount());

        cloud.setAvailable(false);
        gw.ingestJson(sample("dev-001", "temperature", 2, 2));
        gw.ingestJson(sample("dev-002", "pressure", 3, 3));
        gw.ingestJson(sample("dev-001", "temperature", 4, 4));
        assertEquals(3, gw.pendingCount());
        ScenarioLog.step("断连", "3 条采样, 链路 DOWN", "上联失败->入本地有界缓冲并落 WAL",
                "pending=3, 云端接收=" + cloud.acceptedCount());

        cloud.setAvailable(true);
        int confirmed = gw.flushPending();
        assertEquals(3, confirmed);
        assertEquals(0, gw.pendingCount());

        List<String> expectedOrder = List.of(
                "dev-001|temperature|2", "dev-002|pressure|3", "dev-001|temperature|4");
        List<String> tail = cloud.arrivalOrder()
                .subList(cloud.arrivalOrder().size() - 3, cloud.arrivalOrder().size());
        assertEquals(expectedOrder, tail);
        ScenarioLog.summary("断连补传",
                "恢复后按 FIFO 补传，全局交错顺序与设备内顺序均稳定: " + tail);
    }

    @Test
    void ackLossDoesNotDuplicateOrLose(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        cloud.setAvailable(false);
        gw.ingestJson(sample("dev-001", "temperature", 5, 10));

        cloud.setAvailable(true);
        cloud.simulateAckLossOnce();
        int first = gw.flushPending();
        assertEquals(0, first, "ACK 丢失，记录必须仍留在缓冲");
        assertEquals(1, gw.pendingCount());
        assertTrue(cloud.hasReceived("dev-001|temperature|10"), "云端实际已收到该数据");
        ScenarioLog.step("抖动", "ACK 丢失一次", "未获确认不出队，重发由云端幂等去重",
                "pending 保持 1");

        int second = gw.flushPending();
        assertEquals(1, second);
        assertEquals(0, gw.pendingCount());
        assertEquals(1, cloud.acceptedCount(), "重发被云端去重，不产生第二条");
        ScenarioLog.summary("ACK抖动", "重发后确认出队；云端只保留一份，不丢不重");
    }

    @Test
    void duplicateAckCannotRemoveNewHead(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        cloud.setAvailable(false);
        gw.ingestJson(sample("dev-001", "temperature", 1, 100));
        gw.ingestJson(sample("dev-001", "temperature", 2, 200));

        assertFalse(gw.buffer().ack("stale-key-not-head"), "非队头 key 的确认必须无效");
        assertEquals(2, gw.pendingCount(), "迟到/重复确认不得误删任何数据");
        ScenarioLog.summary("重复确认", "过期 ACK 被忽略，队头指针不被污染");
    }

    @Test
    void boundedEvictionIsOldestFirstAndQueryable(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 3, null, holder);
        TelemetryGateway gw = holder[0];

        cloud.setAvailable(false);
        for (int i = 1; i <= 5; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 100L));
        }
        assertEquals(3, gw.pendingCount(), "缓冲占用不得超过容量");
        assertEquals(2, gw.evictionLog().all().size(), "被淘汰记录必须可查询");
        List<Long> evictedTs = gw.evictionLog().all().stream().map(e -> e.record().timestamp()).toList();
        assertEquals(List.of(100L, 200L), evictedTs, "淘汰规则必须是 OLDEST_FIRST");
        assertEquals("OLDEST_FIRST", gw.evictionLog().all().get(0).policy());

        List<Long> remainTs = gw.buffer().snapshot().stream().map(TelemetryRecord::timestamp).toList();
        assertEquals(List.of(300L, 400L, 500L), remainTs);
        ScenarioLog.summary("有界淘汰", "容量=3，写满淘汰最旧两条并记入 evictions.jsonl，占用有硬上界");
    }

    @Test
    void restartRecoversPendingAndReplaysNoDuplicates(@TempDir Path dir) {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 100, null, holder);
        TelemetryGateway gw = holder[0];

        cloud.setAvailable(false);
        for (int i = 1; i <= 4; i++) {
            gw.ingestJson(sample("dev-001", "temperature", i, i * 1000L));
        }
        assertEquals(4, gw.pendingCount());
        ScenarioLog.step("重启", "断连中 4 条未确认, 进程退出", "记录已在 pending.jsonl 落盘",
                "模拟进程重启...");

        SimulatedCloud cloud2 = GatewayTestSupport.restart(dir, 100, holder);
        TelemetryGateway revived = holder[0];
        assertEquals(4, revived.pendingCount(), "重启后未确认数据必须从 WAL 完整恢复");

        cloud2.setAvailable(true);
        int confirmed = revived.flushPending();
        assertEquals(4, confirmed);
        assertEquals(0, revived.pendingCount());
        assertEquals(4, cloud2.acceptedCount());

        int confirmedAgain = revived.flushPending();
        assertEquals(0, confirmedAgain, "再次补传为空，无重复上报");
        ScenarioLog.summary("重启恢复", "WAL 恢复 4 条 -> 有序补传 -> 重复 drain 为空");
    }

    @Test
    void concurrentMultiDevicesLoseNothingDuplicateNothing(@TempDir Path dir) throws Exception {
        SimulatedCloud cloud = GatewayTestSupport.newGateway(dir, 500, null, holder);
        TelemetryGateway gw = holder[0];

        cloud.setAvailable(false);
        int devices = 2;
        int metricsPerDevice = 2;
        int perMetric = 50;
        int batchesPerLane = 5;
        int batchSize = perMetric / batchesPerLane;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch done = new CountDownLatch(devices * metricsPerDevice * batchesPerLane);
        List<Throwable> errors = new ArrayList<>();
        List<Job> jobs = List.of(
                new Job("dev-001", "temperature"), new Job("dev-001", "humidity"),
                new Job("dev-002", "pressure"), new Job("dev-002", "temperature"));
        for (int laneIndex = 0; laneIndex < jobs.size(); laneIndex++) {
            Job job = jobs.get(laneIndex);
            final int laneBase = laneIndex * 1_000_000;
            CountDownLatch[] batchDone = new CountDownLatch[batchesPerLane];
            for (int b = 0; b < batchesPerLane; b++) {
                batchDone[b] = new CountDownLatch(1);
            }
            for (int batch = 0; batch < batchesPerLane; batch++) {
                final CountDownLatch waitFor = batch == 0 ? null : batchDone[batch - 1];
                final CountDownLatch finished = batchDone[batch];
                final int batchStart = batch * batchSize + 1;
                pool.submit(() -> {
                    try {
                        if (waitFor != null) {
                            assertTrue(waitFor.await(30, TimeUnit.SECONDS), "同设备上一批必须先完成");
                        }
                        for (int i = batchStart; i < batchStart + batchSize; i++) {
                            long ts = laneBase + i * 10_000L;
                            gw.ingestJson(sample(job.deviceId(), job.metric(), i, ts));
                        }
                    } catch (Throwable t) {
                        synchronized (errors) {
                            errors.add(t);
                        }
                    } finally {
                        finished.countDown();
                        done.countDown();
                    }
                });
            }
        }
        assertTrue(done.await(30, TimeUnit.SECONDS));
        pool.shutdown();
        assertTrue(errors.isEmpty(), "并发上报不应产生异常: " + errors);

        int total = devices * metricsPerDevice * perMetric;
        assertEquals(total, gw.pendingCount(), "并发入缓冲总数必须精确");

        cloud.setAvailable(true);
        gw.flushPending();
        assertEquals(0, gw.pendingCount());
        assertEquals(total, cloud.acceptedCount(), "云端去重后接收总数必须等于上报总数");

        for (Job job : jobs) {
            String prefix = job.deviceId() + "|" + job.metric() + "|";
            List<String> order = cloud.arrivalOrder().stream()
                    .filter(k -> k.startsWith(prefix)).toList();
            List<String> expected = new ArrayList<>();
            final int laneBaseFinal = jobs.indexOf(job) * 1_000_000;
            for (int i = 1; i <= perMetric; i++) {
                expected.add(prefix + (laneBaseFinal + i * 10_000L));
            }
            assertEquals(expected, order, job.deviceId() + "/" + job.metric() + " 顺序必须稳定");
        }
        ScenarioLog.summary("并发多设备",
                "2 设备 x 2 指标 x " + perMetric + " 条并发：不丢不重，每设备指标到达顺序严格按时间戳递增");
    }

    private record Job(String deviceId, String metric) {
    }
}
