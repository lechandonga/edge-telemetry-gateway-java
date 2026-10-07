package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectCode;

/**
 * 批内单条记录的云端回执。
 *
 * @param index  该记录在请求批次中的下标（从 0 开始）
 * @param key    记录幂等键，网关据此核对“回执对应的确是发出去的那条”
 * @param status 单条结论：ACCEPTED / RETRYABLE / PERMANENTLY_REJECTED
 * @param code   永久拒收时的分类原因；其余状态为 null
 * @param detail 云端给出的可读说明，原样落到隔离记录便于对账核查
 */
public record RecordReceipt(
        int index,
        String key,
        UplinkStatus status,
        CloudRejectCode code,
        String detail
) {
    public static RecordReceipt accepted(int index, String key) {
        return new RecordReceipt(index, key, UplinkStatus.ACCEPTED, null, null);
    }

    public static RecordReceipt retryable(int index, String key, String detail) {
        return new RecordReceipt(index, key, UplinkStatus.RETRYABLE,
                CloudRejectCode.RETRYABLE, detail);
    }

    public static RecordReceipt rejected(int index, String key, CloudRejectCode code, String detail) {
        return new RecordReceipt(index, key, UplinkStatus.PERMANENTLY_REJECTED, code, detail);
    }
}
