package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.config;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * 已登记设备注册表（本地模拟）。未登记设备上报将被拒收并写入死信日志。
 */
public class DeviceRegistry {

    private final Map<String, DeviceSpec> devices;

    public DeviceRegistry(Collection<DeviceSpec> deviceSpecs) {
        this.devices = deviceSpecs.stream().collect(java.util.stream.Collectors
                .toUnmodifiableMap(DeviceSpec::deviceId, d -> d));
    }

    public Optional<DeviceSpec> find(String deviceId) {
        return Optional.ofNullable(devices.get(deviceId));
    }

    public static DeviceRegistry defaultRegistry() {
        MetricSpec temperature = new MetricSpec("temperature", "C", -40, 125, -20, 80);
        MetricSpec humidity = new MetricSpec("humidity", "%", 0, 100, 5, 95);
        MetricSpec pressure = new MetricSpec("pressure", "kPa", 0, 200, 50, 150);
        return new DeviceRegistry(java.util.List.of(
                new DeviceSpec("dev-001", "冷链传感器A",
                        Map.of("temperature", temperature, "humidity", humidity),
                        Map.of("R01", "temperature", "R02", "humidity")),
                new DeviceSpec("dev-002", "压力传感器B",
                        Map.of("pressure", pressure, "temperature", temperature),
                        Map.of("R01", "pressure", "R02", "temperature"))
        ));
    }
}
