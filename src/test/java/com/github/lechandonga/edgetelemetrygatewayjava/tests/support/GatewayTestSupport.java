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
        SimulatedCloud cloud = new SimulatedCloud();
        RuleStore ruleStore = new RuleStore(dataDir);
        if (initialRules != null) {
            ruleStore.publish(initialRules);
        }
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        TelemetryGateway gateway = new TelemetryGateway(dataDir, capacity,
                DeviceRegistry.defaultRegistry(), cloud, alertEngine);
        holder[0] = gateway;
        return cloud;
    }

    /** 模拟进程重启：用同一数据目录重建全部组件并从磁盘恢复。 */
    public static SimulatedCloud restart(Path dataDir, int capacity, TelemetryGateway[] holder) {
        SimulatedCloud cloud = new SimulatedCloud();
        RuleStore ruleStore = new RuleStore(dataDir);
        AlertEngine alertEngine = new AlertEngine(dataDir, ruleStore);
        holder[0] = new TelemetryGateway(dataDir, capacity,
                DeviceRegistry.defaultRegistry(), cloud, alertEngine);
        return cloud;
    }
}
