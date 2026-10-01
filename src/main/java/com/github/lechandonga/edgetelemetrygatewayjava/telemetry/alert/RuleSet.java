package com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert;

import java.util.List;

/**
 * 一版规则配置。version 单调（如 v1/v2 或时间戳），
 * 每条规则也携带所属配置版本，保证告警结论可追溯到规则版本。
 */
public record RuleSet(String version, long createdAt, List<ThresholdRule> rules) {
}
