package com.github.lechandonga.edgetelemetrygatewayjava.tests.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用例场景日志：统一格式打印“输入 → 判定依据 → 关键状态变化”。
 */
public final class ScenarioLog {

    private static final Logger log = LoggerFactory.getLogger("SCENARIO");

    private ScenarioLog() {
    }

    public static void step(String scenario, String input, String basis, String stateChange) {
        log.info("[{}] 输入={} | 判定依据={} | 状态变化={}", scenario, input, basis, stateChange);
    }

    public static void summary(String scenario, String summary) {
        log.info("[{}] 结论: {}", scenario, summary);
    }
}
