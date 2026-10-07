package com.github.lechandonga.edgetelemetrygatewayjava.tests.support;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleSet;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.UplinkManager;

import java.nio.file.Path;

public final class GatewayTestSupport {

    private GatewayTestSupport() {
    }

    public static SimulatedCloud newGateway(Path dataDir, int capacity, RuleSet initialRules,
                                            TelemetryGateway[] holder) {
        return newGateway(dataDir, capacity, initialRules, DeviceRegistry.defaultRegistry(),
                UplinkManager.DEFAULT_BATCH_SIZE, holder);
    }

    public static SimulatedCloud newGateway(Path dataDir, int capacity, RuleSet initialRules,
                                            DeviceRegistry registry, int batchSize,
                                            TelemetryGateway[] holder) {
        SimulatedCloud cloud = new SimulatedCloud();
        RuleStore ruleStore = new RuleStore(dataDir);
        if (initialRules != null) {
            ruleStore.publish(initialRules);
        }
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        TelemetryGateway gateway = new TelemetryGateway(dataDir, capacity,
                registry, cloud, alertEngine, batchSize);
        holder[0] = gateway;
        return cloud;
    }

    /** 模拟进程重启：用同一数据目录重建全部组件并从磁盘恢复。 */
    public static SimulatedCloud restart(Path dataDir, int capacity, TelemetryGateway[] holder) {
        return restart(dataDir, capacity, DeviceRegistry.defaultRegistry(),
                UplinkManager.DEFAULT_BATCH_SIZE, holder);
    }

    /** 模拟进程重启（指定注册表与批大小）：用同一数据目录重建全部组件并从磁盘恢复。 */
    public static SimulatedCloud restart(Path dataDir, int capacity, DeviceRegistry registry,
                                         int batchSize, TelemetryGateway[] holder) {
        SimulatedCloud cloud = new SimulatedCloud();
        return restart(dataDir, capacity, registry, batchSize, cloud, holder);
    }

    /** 模拟进程重启但复用同一个云端（云端幂等键状态保留，用于核验重发不重）。 */
    public static SimulatedCloud restart(Path dataDir, int capacity, DeviceRegistry registry,
                                         int batchSize, SimulatedCloud cloud,
                                         TelemetryGateway[] holder) {
        RuleStore ruleStore = new RuleStore(dataDir);
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        holder[0] = new TelemetryGateway(dataDir, capacity,
                registry, cloud, alertEngine, batchSize);
        return cloud;
    }
}
