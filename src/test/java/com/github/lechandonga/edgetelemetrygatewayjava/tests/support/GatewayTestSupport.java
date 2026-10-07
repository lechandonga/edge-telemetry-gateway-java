package com.github.lechandonga.edgetelemetrygatewayjava.tests.support;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleSet;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config.DeviceRegistry;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.gateway.TelemetryGateway;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink.SimulatedCloud;

import java.nio.file.Path;

public final class GatewayTestSupport {

    private GatewayTestSupport() {
    }

    public static SimulatedCloud newGateway(Path dataDir, int capacity, RuleSet initialRules,
                                            TelemetryGateway[] holder) {
        return newGatewayBatched(dataDir, capacity, initialRules, holder, 1);
    }

    /** 批量补传装配：batchSize 指定补传批大小（1 为原有逐条语义）。 */
    public static SimulatedCloud newGatewayBatched(Path dataDir, int capacity, RuleSet initialRules,
                                                   TelemetryGateway[] holder, int batchSize) {
        SimulatedCloud cloud = new SimulatedCloud();
        RuleStore ruleStore = new RuleStore(dataDir);
        if (initialRules != null) {
            ruleStore.publish(initialRules);
        }
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        TelemetryGateway gateway = new TelemetryGateway(dataDir, capacity,
                DeviceRegistry.defaultRegistry(), cloud, alertEngine, batchSize);
        holder[0] = gateway;
        return cloud;
    }

    /** 模拟进程重启：用同一数据目录重建全部组件并从磁盘恢复。 */
    public static SimulatedCloud restart(Path dataDir, int capacity, TelemetryGateway[] holder) {
        return restart(dataDir, capacity, holder, 1);
    }

    /** 模拟进程重启（批量补传装配）。 */
    public static SimulatedCloud restart(Path dataDir, int capacity, TelemetryGateway[] holder,
                                         int batchSize) {
        SimulatedCloud cloud = new SimulatedCloud();
        RuleStore ruleStore = new RuleStore(dataDir);
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        holder[0] = new TelemetryGateway(dataDir, capacity,
                DeviceRegistry.defaultRegistry(), cloud, alertEngine, batchSize);
        return cloud;
    }

    /** 使用自定义设备注册表装配（真并发大批量场景需要 8+ 设备）。 */
    public static SimulatedCloud newGateway(Path dataDir, int capacity, RuleSet initialRules,
                                            DeviceRegistry registry, TelemetryGateway[] holder,
                                            int batchSize) {
        SimulatedCloud cloud = new SimulatedCloud();
        RuleStore ruleStore = new RuleStore(dataDir);
        if (initialRules != null) {
            ruleStore.publish(initialRules);
        }
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        holder[0] = new TelemetryGateway(dataDir, capacity, registry, cloud, alertEngine, batchSize);
        return cloud;
    }
}
