package com.github.lechandonga.edgetelemetrygatewayjava.ingest;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 设备登记表。未登记设备的记录以 UNKNOWN_DEVICE 拒收。 */
public class DeviceRegistry {

    private final Map<String, DeviceSpec> devices = new ConcurrentHashMap<>();

    public void register(DeviceSpec spec) {
        devices.put(spec.deviceId(), spec);
    }

    public Optional<DeviceSpec> find(String deviceId) {
        return deviceId == null ? Optional.empty() : Optional.ofNullable(devices.get(deviceId));
    }

    public boolean isRegistered(String deviceId) {
        return find(deviceId).isPresent();
    }
}
