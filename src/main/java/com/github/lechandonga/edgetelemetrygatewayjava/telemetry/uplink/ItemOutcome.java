package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.uplink;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.CloudRejectReason;

import java.util.Optional;

/**
 * 云端对批内单条数据的处理结果。
 *
 * 三种状态必须严格区分：
 * <ul>
 *   <li>{@link #accepted(String)}：云端确认接收，数据可以出队；</li>
 *   <li>{@link #permanentlyRejected(CloudRejectReason, String)}：云端明确永远不接收，
 *       数据转入隔离区后从补传队列移除；</li>
 *   <li>{@link #unresolved()}：暂时发不出去（断连/ACK 丢失/云端未回执该条），
 *       数据必须留在队列，后续批次重发。</li>
 * </ul>
 *
 * 批量回执按批内顺序逐条解释：一旦遇到第一条 {@code unresolved}，
 * 其后的结果一律视为不可信，对应数据全部保留在队列（见 {@link BatchReceipt}）。
 *
 * @param ack    云端幂等回执，仅 accepted 非空
 * @param reason 永久拒收分类，仅 permanentlyRejected 非空
 * @param detail 永久拒收说明，可空
 */
public record ItemOutcome(Status status, String ack, CloudRejectReason reason, String detail) {

    public enum Status {
        ACCEPTED,
        PERMANENTLY_REJECTED,
        UNRESOLVED
    }

    public static ItemOutcome accepted(String ack) {
        return new ItemOutcome(Status.ACCEPTED, ack, null, null);
    }

    public static ItemOutcome permanentlyRejected(CloudRejectReason reason, String detail) {
        return new ItemOutcome(Status.PERMANENTLY_REJECTED, null, reason, detail);
    }

    public static ItemOutcome unresolved() {
        return new ItemOutcome(Status.UNRESOLVED, null, null, null);
    }

    public boolean accepted() {
        return status == Status.ACCEPTED;
    }

    public boolean permanentlyRejected() {
        return status == Status.PERMANENTLY_REJECTED;
    }

    public Optional<String> ackOptional() {
        return Optional.ofNullable(ack);
    }
}
