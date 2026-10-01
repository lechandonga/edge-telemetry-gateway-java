package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

import java.util.HashMap;
import java.util.Map;

/**
 * 全部规则窗口状态快照，进程重启后整体恢复。
 * key = ruleId|deviceId|metric。
 */
public class StateSnapshot {
    public String currentVersion;
    public Map<String, RuleState> states = new HashMap<>();
}
