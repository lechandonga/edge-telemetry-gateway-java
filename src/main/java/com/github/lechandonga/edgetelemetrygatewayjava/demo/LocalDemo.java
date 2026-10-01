package com.github.lechandonga.edgetelemetrygatewayjava.demo;

import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertEventStore;
import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertReplay;
import com.github.lechandonga.edgetelemetrygatewayjava.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.buffer.TelemetryBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.DeviceSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.RegisterSample;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertEvent;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertRule;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.pipeline.Gateway;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.CloudIngestService;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.UplinkLink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * 纯本地端到端模拟：多设备并发上报（寄存器 + JSON）、断连积压、恢复补传、
 * 重复确认、缓冲淘汰、规则版本更新、进程重启恢复、历史重放复现。
 * 不依赖任何外部服务，状态写入本地目录。
 *
 * 运行：mvn -q exec:java 不可用时可直接运行本类 main（IDE），或
 * mvn -q compile exec:java -Dexec.mainClass=...LocalDemo（需 exec 插件）；
 * 最简单：mvn test（同一套场景由测试自动验证）。
 */
public final class LocalDemo {

    private LocalDemo() {
    }

    public record Context(Gateway gateway, RuleStore rules, TelemetryBuffer buffer,
                          CloudIngestService cloud, UplinkLink link) {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Path.of("target/demo-state");
        clean(dir);
        run(dir, System.out::println);
    }

    public static void run(Path stateDir, Consumer<String> log) throws Exception {
        DeviceSpec specA = new DeviceSpec("dev-A",
                Map.of(40001, "temperature", 40002, "vibration"),
                Map.of("temperature", new double[]{-40, 125},
                        "vibration", new double[]{0, 100}));
        DeviceSpec specB = new DeviceSpec("dev-B",
                Map.of(40001, "temperature"),
                Map.of("temperature", new double[]{-40, 125}));

        CloudIngestService cloud = new CloudIngestService();
        UplinkLink link = new UplinkLink();
        Context ctx = build(stateDir, specA, specB, cloud, link, log, 50);
        Gateway gw = ctx.gateway();
        AlertRule v1 = ctx.rules().publish("temperature", 80.0, 1_000L, 1_000L);

        log.accept("==== phase 1: uplink up, mixed valid/invalid input ====");
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 25.0, 1_000L));
        gw.ingestJson("{\"deviceId\":\"dev-B\",\"metric\":\"temperature\",\"value\":26.0,\"ts\":1000}");
        gw.ingestJson("{not-json");
        gw.ingestRegister(new RegisterSample("dev-X", 40001, 1.0, 1_000L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 999.0, 1_000L));
        gw.ingestJson("{\"deviceId\":\"dev-A\",\"metric\":\"temperature\"}");
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 30.0, 500L));

        log.accept("==== phase 2: link down, concurrent multi-device burst (capacity=50) ====");
        link.disconnect();
        ExecutorService pool = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 30; i++) {
            final int n = i;
            pool.submit(() -> {
                String dev = n % 2 == 0 ? "dev-A" : "dev-B";
                gw.ingestRegister(new RegisterSample(dev, 40001, 20.0 + (n % 10), 2_000L + n));
            });
            pool.submit(() -> gw.ingestJson(
                    "{\"deviceId\":\"dev-A\",\"metric\":\"vibration\",\"value\":5.0,\"ts\":2000}"));
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        log.accept("[demo] buffer stats after burst: " + gw.bufferStats());
        log.accept("[demo] eviction journal lines: " + ctx.buffer().evictionJournal().size());

        log.accept("==== phase 3: link restored, ordered replay ====");
        link.connect();
        int delivered = gw.onLinkRestored();
        log.accept("[demo] cloud received " + cloud.receivedCount() + ", replay delivered " + delivered);

        log.accept("==== phase 4: duplicate replay & ack idempotency ====");
        List<?> results = gw.forceReplay();
        log.accept("[demo] forced replay on drained buffer outcomes=" + results
                + " cloudCount=" + cloud.receivedCount());

        log.accept("==== phase 5: duration window + jitter dedup ====");
        link.disconnect();
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 85.0, 5_000L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 90.0, 5_500L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 91.0, 6_000L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 92.0, 6_500L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 10.0, 7_000L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 88.0, 7_200L));
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 89.0, 8_300L));
        link.connect();
        gw.onLinkRestored();

        log.accept("==== phase 6: rule version update, in-flight window pinned ====");
        AlertRule v2 = ctx.rules().publish("temperature", 60.0, 5_000L, 9_000L);
        log.accept("[demo] rules now: v1=" + v1 + " v2=" + v2);
        gw.ingestRegister(new RegisterSample("dev-A", 40001, 65.0, 9_000L));

        log.accept("==== phase 7: simulated process restart ====");
        Context restarted = build(stateDir, specA, specB, cloud, link, log, 50);
        Gateway gw2 = restarted.gateway();
        log.accept("[demo] buffer stats after restart: " + gw2.bufferStats());
        gw2.ingestRegister(new RegisterSample("dev-A", 40001, 66.0, 9_200L));
        log.accept("[demo] persisted alert events across restart: "
                + gw2.alertEngine().firedEvents().size());

        log.accept("==== phase 8: deterministic historical replay ====");
        List<TelemetryRecord> history = cloud.received();
        List<AlertEvent> replayV1 = AlertReplay.replay(history, v1);
        List<AlertEvent> replayV1Again = AlertReplay.replay(history, v1);
        List<AlertEvent> replayV2 = AlertReplay.replay(history, v2);
        log.accept("[demo] replay v1 events=" + replayV1.size()
                + " deterministic=" + replayV1.equals(replayV1Again)
                + " | replay v2 events=" + replayV2.size());

        log.accept("==== demo summary ====");
        log.accept("cloudReceived=" + cloud.receivedCount()
                + " alerts=" + gw2.alertEngine().firedEvents().size()
                + " buffer=" + gw2.bufferStats());
    }

    public static Context build(Path stateDir, DeviceSpec specA, DeviceSpec specB,
                                CloudIngestService cloud, UplinkLink link,
                                Consumer<String> log, int capacity) {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(specA);
        registry.register(specB);
        Normalizer normalizer = new Normalizer(registry);
        TelemetryBuffer buffer = new TelemetryBuffer(stateDir.resolve("buffer"), capacity);
        RuleStore rules = new RuleStore(stateDir.resolve("alert"));
        AlertEventStore eventStore = new AlertEventStore(stateDir.resolve("alert"));
        AlertEngine engine = new AlertEngine(rules, eventStore, stateDir.resolve("alert"), log);
        Gateway gateway = new Gateway(normalizer, buffer, engine, link, cloud, log);
        return new Context(gateway, rules, buffer, cloud, link);
    }

    private static void clean(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                }
            });
        }
    }
}
