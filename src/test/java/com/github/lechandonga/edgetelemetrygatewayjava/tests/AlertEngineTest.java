package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEngine;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.AlertEvent;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleSet;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.RuleStore;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.alert.ThresholdRule;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.Quality;
import com.github.lechandonga.edgetelemetrygatewayjava.telemetry.model.TelemetryRecord;
import com.github.lechandonga.edgetelemetrygatewayjava.tests.support.ScenarioLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 持续时长窗口、抖动不重复告警、规则版本更新（窗口迁移/确定性重放）、跨重启状态保持。
 */
class AlertEngineTest {

    private ThresholdRule tempHigh(double threshold, long duration) {
        return new ThresholdRule("high-temp", "dev-001", "temperature",
                ThresholdRule.Operator.GT, threshold, duration);
    }

    private TelemetryRecord rec(double value, long ts) {
        return new TelemetryRecord(0, "dev-001", "temperature", value, ts,
                "JSON", Quality.GOOD, List.of());
    }

    private long fires(List<AlertEvent> events) {
        return events.stream().filter(e -> AlertEvent.FIRE.equals(e.type())).count();
    }

    @Test
    void durationWindowAndFlapDeduplication(@TempDir Path dir) {
        RuleStore store = new RuleStore(dir);
        store.publish(new RuleSet("v1", 0, List.of(tempHigh(30.0, 3_000))));
        AlertEngine engine = new AlertEngine(dir, store);

        assertTrue(engine.process(rec(35, 1_000)).isEmpty(), "刚越限只是进入观察窗口，不能立刻告警");
        assertTrue(engine.process(rec(20, 2_000)).isEmpty(), "窗口内提前恢复，窗口作废");
        ScenarioLog.step("窗口抖动", "35@1s 越限 -> 20@2s 恢复",
                "持续未达 3s 窗口即恢复", "PENDING->NORMAL，无 FIRE");

        engine.process(rec(35, 3_000));
        List<AlertEvent> e4 = engine.process(rec(36, 5_000));
        assertTrue(e4.isEmpty(), "3s->5s 只持续 2s，仍在观察窗口");
        List<AlertEvent> e5 = engine.process(rec(40, 7_000));
        assertEquals(1, fires(e5), "3s->7s 持续 4s 达 3s 窗口，恰好产生一次 FIRE");

        List<AlertEvent> e5b = engine.process(rec(41, 9_000));
        assertTrue(e5b.isEmpty(), "FIRED 后继续越限不得重复告警");

        List<AlertEvent> e6 = engine.process(rec(20, 10_000));
        engine.process(rec(35, 11_000));
        List<AlertEvent> e8 = engine.process(rec(40, 15_000));
        assertEquals(AlertEvent.RECOVER, e6.get(0).type());
        assertEquals(1, fires(e8), "先恢复后再次持续越限，才允许第二次 FIRE");

        ScenarioLog.summary("持续窗口+抖动", "全程仅 2 次 FIRE（两个独立越限区间），反复抖动不产生重复告警");
    }

    @Test
    void ruleUpdateCarriesOpenWindowWithoutDroppingOrReFiring(@TempDir Path dir) {
        RuleStore store = new RuleStore(dir);
        store.publish(new RuleSet("v1", 0, List.of(tempHigh(30.0, 5_000))));
        AlertEngine engine = new AlertEngine(dir, store);

        engine.process(rec(35, 1_000));
        engine.process(rec(36, 3_000));
        ScenarioLog.step("规则更新", "窗口已从 1s 持续到 3s（PENDING），期间发布 v2（阈值 25，时长 5s）",
                "同 ruleId 的进行中窗口按 ruleId 迁移，窗口不重置", "发布 v2");

        engine.publishRules(new RuleSet("v2", 1, List.of(tempHigh(25.0, 5_000))));

        List<AlertEvent> after = engine.process(rec(37, 6_500));
        assertEquals(1, fires(after), "沿用更新前打开的窗口：6.5s-1s 达 5s，只产生一次 FIRE");
        assertEquals("v2", after.get(0).ruleVersion(), "FIRE 归属发布时的规则版本 v2");
        ScenarioLog.summary("规则热更新", "进行中窗口未被丢弃也未重新计时；结论明确归属新版本 v2");
    }

    @Test
    void sameHistorySameVersionReplaysDeterministically(@TempDir Path dir) {
        RuleStore store = new RuleStore(dir);
        RuleSet v1 = new RuleSet("v1", 0, List.of(tempHigh(30.0, 2_000)));
        RuleSet v2 = new RuleSet("v2", 1, List.of(tempHigh(28.0, 2_000)));
        store.publish(v1);
        store.publish(v2);

        List<TelemetryRecord> history = List.of(
                rec(29, 1_000), rec(29, 2_000), rec(29, 4_000), rec(20, 6_000));

        List<AlertEvent> runA = AlertEngine.replay(history, store.version("v1").orElseThrow());
        List<AlertEvent> runB = AlertEngine.replay(history, store.version("v1").orElseThrow());
        List<AlertEvent> runV2 = AlertEngine.replay(history, store.version("v2").orElseThrow());

        assertEquals(0, fires(runA), "v1 阈值 30：29 不越限，无告警");
        assertEquals(1, fires(runV2), "v2 阈值 28：同段历史应产生一次告警");
        assertEquals(runA.stream().map(AlertEvent::type).toList(),
                runB.stream().map(AlertEvent::type).toList(), "同版本重放两次，结论必须完全一致");
        assertEquals("v2", runV2.stream().filter(e -> AlertEvent.FIRE.equals(e.type()))
                .findFirst().orElseThrow().ruleVersion());
        ScenarioLog.summary("版本化重放",
                "同段历史：v1 无告警、v2 有 1 次 FIRE；v1 重放两次结论一致，结论可复现可解释");
    }

    @Test
    void stateSurvivesRestartAndFiresExactlyOnce(@TempDir Path dir) {
        RuleStore store = new RuleStore(dir);
        store.publish(new RuleSet("v1", 0, List.of(tempHigh(30.0, 5_000))));
        AlertEngine engine = new AlertEngine(dir, store);

        engine.process(rec(35, 1_000));
        engine.process(rec(36, 3_000));

        AlertEngine revived = new AlertEngine(dir, new RuleStore(dir));
        ScenarioLog.step("告警重启恢复", "重启前 PENDING 窗口 [1s,3s]",
                "alert-state.json 快照恢复窗口起点", "重启后继续处理 6s 采样");

        List<AlertEvent> events = revived.process(rec(37, 6_500));
        assertEquals(1, fires(events), "重启不丢失已持续窗口，达时长后只 FIRE 一次");
        assertFalse(revived.process(rec(40, 8_000)).stream()
                .anyMatch(e -> AlertEvent.FIRE.equals(e.type())), "重启后 FIRED 状态同样保持，不重复触发");
        ScenarioLog.summary("跨重启告警", "PENDING 窗口与 FIRED 状态均跨重启保持，全程恰好一次 FIRE");
    }
}
