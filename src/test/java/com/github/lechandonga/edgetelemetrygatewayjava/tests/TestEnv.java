package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertEventStore;
import com.github.lechandonga.edgetelemetrygatewayjava.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.buffer.TelemetryBuffer;
import com.github.lechandonga.edgetelemetrygatewayjava.demo.LocalDemo;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.DeviceSpec;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer;
import com.github.lechandonga.edgetelemetrygatewayjava.pipeline.Gateway;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.CloudIngestService;
import com.github.lechandonga.edgetelemetrygatewayjava.uplink.UplinkLink;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** 测试装配：每次独立临时目录、共享本地文件状态，无任何外部服务。 */
public record TestEnv(Path dir, DeviceRegistry registry, TelemetryBuffer buffer,
                      RuleStore rules, AlertEventStore eventStore, AlertEngine engine,
                      Gateway gateway, CloudIngestService cloud, UplinkLink link) {

    public static DeviceSpec specA() {
        return new DeviceSpec("dev-A",
                Map.of(40001, "temperature", 40002, "vibration"),
                Map.of("temperature", new double[]{-40, 125},
                        "vibration", new double[]{0, 100}));
    }

    public static DeviceSpec specB() {
        return new DeviceSpec("dev-B",
                Map.of(40001, "temperature"),
                Map.of("temperature", new double[]{-40, 125}));
    }

    public static TestEnv create(Path dir, int capacity,
                                 Normalizer.OutOfOrderPolicy policy) {
        return create(dir, capacity, policy, s -> { });
    }

    public static TestEnv create(Path dir, int capacity,
                                 Normalizer.OutOfOrderPolicy policy,
                                 Consumer<String> logger) {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(specA());
        registry.register(specB());
        Normalizer normalizer = new Normalizer(registry, policy);
        TelemetryBuffer buffer = new TelemetryBuffer(dir.resolve("buffer"), capacity);
        RuleStore rules = new RuleStore(dir.resolve("alert"));
        AlertEventStore events = new AlertEventStore(dir.resolve("alert"));
        AlertEngine engine = new AlertEngine(rules, events, dir.resolve("alert"), logger);
        CloudIngestService cloud = new CloudIngestService();
        UplinkLink link = new UplinkLink();
        Gateway gateway = new Gateway(normalizer, buffer, engine, link, cloud, logger);
        return new TestEnv(dir, registry, buffer, rules, events, engine, gateway, cloud, link);
    }

    /** 模拟进程重启：所有持久化组件从同一目录重建。 */
    public TestEnv restart(Consumer<String> logger) {
        DeviceRegistry registry = new DeviceRegistry();
        registry.register(specA());
        registry.register(specB());
        Normalizer normalizer = new Normalizer(registry);
        TelemetryBuffer buffer = new TelemetryBuffer(dir.resolve("buffer"),
                buffer().stats().capacity());
        RuleStore rules = new RuleStore(dir.resolve("alert"));
        AlertEventStore events = new AlertEventStore(dir.resolve("alert"));
        AlertEngine engine = new AlertEngine(rules, events, dir.resolve("alert"), logger);
        Gateway gateway = new Gateway(normalizer, buffer, engine, link, cloud, logger);
        return new TestEnv(dir, registry, buffer, rules, events, engine, gateway, cloud, link);
    }

    public static void unused() {
        List.of(LocalDemo.class);
    }
}
