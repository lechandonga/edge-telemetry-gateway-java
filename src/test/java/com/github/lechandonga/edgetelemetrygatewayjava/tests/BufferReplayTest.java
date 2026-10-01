package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.buffer.AckOutcome;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.RegisterSample;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 断连补传、重复确认幂等、重启恢复、抖动、FIFO 淘汰、多设备并发不丢不重。 */
class BufferReplayTest {

    private final List<String> logs = new ArrayList<>();

    @Test
    void disconnectBuffersAndReplayInOrder(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        for (int i = 0; i < 5; i++) {
            env.gateway().ingestRegister(
                    new RegisterSample("dev-A", 40001, 20.0 + i, 1000L + i * 10L));
        }
        assertEquals(5, env.buffer().stats().pendingCount());
        assertEquals(0, env.cloud().receivedCount());

        env.link().connect();
        int delivered = env.gateway().onLinkRestored();
        assertEquals(5, delivered);
        assertEquals(0, env.buffer().stats().pendingCount());

        List<Long> cloudOrder = env.cloud().received().stream()
                .map(r -> r.recordId()).toList();
        List<Long> sorted = cloudOrder.stream().sorted().toList();
        assertEquals(sorted, cloudOrder, "补传必须按 recordId 升序");
        printLogs();
    }

    @Test
    void replayIsIdempotentUnderDuplicateAckAndReplay(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        env.gateway().ingestRegister(new RegisterSample("dev-A", 40001, 20.0, 1000L));
        long id = env.buffer().pendingRecords().get(0).recordId();

        env.link().connect();
        assertEquals(1, env.gateway().onLinkRestored());
        // 已确认后再确认：幂等
        assertEquals(AckOutcome.DUPLICATE_ACK, env.buffer().confirm(id));
        // 云端重复接收同一条仍不产生重复存储
        env.gateway().forceReplay();
        assertEquals(1, env.cloud().receivedCount());
        printLogs();
    }

    @Test
    void restartRecoversPendingAndDoesNotDuplicateCloud(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        for (int i = 0; i < 4; i++) {
            env.gateway().ingestRegister(
                    new RegisterSample("dev-A", 40001, 20.0 + i, 1000L + i * 10L));
        }
        // 全程断连后“重启”：恢复后补传，云端恰好一次

        TestEnv after = env.restart(logs::add);
        assertEquals(4, after.buffer().stats().pendingCount());
        after.link().connect();
        after.gateway().onLinkRestored();
        assertEquals(4, after.cloud().receivedCount(), "云端恰好 4 条，无重复");
        assertEquals(0, after.buffer().stats().pendingCount());

        // 再次重启也不会重复
        TestEnv again = after.restart(logs::add);
        again.gateway().onLinkRestored();
        assertEquals(4, again.cloud().receivedCount());
        printLogs();
    }

    @Test
    void flappingLinkDoesNotLoseOrDuplicate(@TempDir Path dir) {
        TestEnv env = base(dir);
        for (int i = 0; i < 20; i++) {
            if (i % 4 == 0) {
                env.link().disconnect();
            }
            if (i % 4 == 2) {
                env.link().connect();
                env.gateway().onLinkRestored();
            }
            env.gateway().ingestRegister(
                    new RegisterSample("dev-A", 40001, 20.0 + (i % 5), 1000 + i * 10L));
            if (env.link().isAvailable()) {
                env.gateway().onLinkRestored();
            }
        }
        env.link().connect();
        env.gateway().onLinkRestored();
        assertEquals(0, env.buffer().stats().pendingCount());
        long uniqueIds = env.cloud().received().stream().map(r -> r.recordId()).distinct().count();
        assertEquals(20, uniqueIds, "抖动期间不丢不重");
        assertEquals(20, env.cloud().receivedCount());
        printLogs();
    }

    @Test
    void boundedBufferEvictsOldestWithQueryableJournal(@TempDir Path dir) {
        TestEnv env = base(dir, 5);
        env.link().disconnect();
        for (int i = 0; i < 8; i++) {
            env.gateway().ingestRegister(
                    new RegisterSample("dev-A", 40001, 20.0 + i, 1000 + i * 10L));
        }
        assertEquals(5, env.buffer().stats().pendingCount(), "占用不超过容量");
        assertEquals(3, env.buffer().stats().totalEvicted());
        assertEquals("EVICT_OLDEST_FIFO", env.buffer().stats().evictionPolicy());
        List<Long> pendingIds = env.buffer().pendingRecords().stream()
                .map(r -> r.recordId()).toList();
        assertEquals(List.of(4L, 5L, 6L, 7L, 8L), pendingIds, "被淘汰的是最早进入的 FIFO 记录");
        assertEquals(3, env.buffer().evictionJournal().size(), "淘汰流水可查询");

        // 重启后统计仍在，且占用仍有界
        TestEnv after = env.restart(logs::add);
        assertEquals(5, after.buffer().stats().pendingCount());
        assertEquals(3, after.buffer().stats().totalEvicted());
        printLogs();
    }

    @Test
    void concurrentMultiDeviceNoLossNoDuplicate(@TempDir Path dir) throws Exception {
        TestEnv env = base(dir);
        env.link().disconnect();
        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    String device = (tid + i) % 2 == 0 ? "dev-A" : "dev-B";
                    env.gateway().ingestRegister(new RegisterSample(
                            device, 40001, 20.0 + (i % 5), 1000 + tid * 1000L + i));
                }
                return null;
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(threads * perThread, env.buffer().stats().pendingCount());

        env.link().connect();
        env.gateway().onLinkRestored();
        long total = threads * perThread;
        assertEquals(total, env.cloud().receivedCount());
        assertEquals(total, env.cloud().received().stream().map(r -> r.recordId()).distinct().count());
        assertEquals(0, env.buffer().stats().pendingCount());

        // 每个设备的 deviceSeq 连续无重复
        long seqA = env.cloud().received().stream().filter(r -> r.deviceId().equals("dev-A"))
                .map(r -> r.deviceSeq()).distinct().count();
        long countA = env.cloud().received().stream().filter(r -> r.deviceId().equals("dev-A"))
                .count();
        assertEquals(countA, seqA, "同设备 deviceSeq 唯一");
        printLogs();
    }

    private TestEnv base(Path dir) {
        return base(dir, 1000);
    }

    private TestEnv base(Path dir, int capacity) {
        return TestEnv.create(dir, capacity,
                com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer.OutOfOrderPolicy.CLAMP,
                logs::add);
    }

    private void printLogs() {
        logs.forEach(System.out::println);
        logs.clear();
    }
}
