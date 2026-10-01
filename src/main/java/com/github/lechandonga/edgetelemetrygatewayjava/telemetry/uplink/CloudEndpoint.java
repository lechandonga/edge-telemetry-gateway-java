package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;

import java.util.Optional;

/**
 * 云端上联端点（本地模拟，无外部服务依赖）。
 */
public interface CloudEndpoint {

    /**
     * 云端接收一条数据。
     *
     * @return 成功返回云端幂等回执；上联不可用返回 empty（网关转入断连缓冲）。
     */
    Optional<String> receive(TelemetryRecord record);

    /** 云端是否可达（模拟断连/恢复）。 */
    boolean isAvailable();

    /** 云端已去重接收的数据条数（用于测试断言“不重”）。 */
    int acceptedCount();
}
