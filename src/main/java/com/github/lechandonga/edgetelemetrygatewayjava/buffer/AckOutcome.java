package com.github.lechandonga.edgetelemetrygatewayjava.buffer;

/** 补传确认结果，用于区分首次确认与重复/未知确认。 */
public enum AckOutcome {
    /** 首次确认，记录已移除。 */
    ACKED,
    /** 重复确认：该记录此前已确认过，幂等空操作。 */
    DUPLICATE_ACK,
    /** 未知 id：从未分配或已被淘汰，幂等空操作。 */
    UNKNOWN_ID
}
