package com.github.lechandonga.edgetelemetrygatewayjava.tests;

import com.github.lechandonga.edgetelemetrygatewayjava.alert.AlertReplay;
import com.github.lechandonga.edgetelemetrygatewayjava.ingest.RegisterSample;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertEvent;
import com.github.lechandonga.edgetelemetrygatewayjava.model.AlertRule;
import com.github.lechandonga.edgetelemetrygatewayjava.model.TelemetryRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 持续时长窗口、抖动去重、跨重启、规则版本更新与历史重放可复现。 */
class AlertRuleVersionTest {

    private final List<String> logs = new ArrayList<>();

    @Test
    void durationWindowFiresOnceAndJitterOpensNewWindow(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        env.rules().publish("temperature", 80.0, 1_000L, 0L);

        send(env, 85.0, 5000);
        send(env, 90.0, 5500);
        assertEquals(0, env.engine().firedEvents().size(), "窗口未满不触发");
        send(env, 91.0, 6000);
        assertEquals(1, env.engine().firedEvents().size(), "持续 1s 触发");
        send(env, 92.0, 6500);
        assertEquals(1, env.engine().firedEvents().size(), "同一窗口不重复告警");

        send(env, 10.0, 7000); // 恢复复位
        send(env, 88.0, 7200);
        send(env, 89.0, 8300); // 新窗口再次触发
        assertEquals(2, env.engine().firedEvents().size());
        printLogs();
    }

    @Test
    void alertStateSurvivesRestartAndDoesNotRefire(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        env.rules().publish("temperature", 80.0, 1_000L, 0L);

        send(env, 85.0, 1000);
        send(env, 86.0, 1500);
        // 重启：窗口进行中
        TestEnv after = env.restart(logs::add);
        after.link().disconnect();
        send(after, 90.0, 2100); // 累计越限达到 1s
        assertEquals(1, after.engine().firedEvents().size());

        // 再次重启：已触发窗口不得重复
        TestEnv again = after.restart(logs::add);
        send(again, 95.0, 2500);
        assertEquals(1, again.engine().firedEvents().size());
        printLogs();
    }

    @Test
    void ruleUpdatePinsInFlightWindowAndNewWindowUsesNewVersion(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        AlertRule v1 = env.rules().publish("temperature", 80.0, 2_000L, 0L);

        send(env, 85.0, 1000); // v1 窗口开窗
        AlertRule v2 = env.rules().publish("temperature", 60.0, 10_000L, 1500L);
        assertEquals(1, v1.version());
        assertEquals(2, v2.version());

        send(env, 86.0, 3100); // 在途窗口仍按 v1 判定并触发
        List<AlertEvent> events = env.engine().firedEvents();
        assertEquals(1, events.size());
        assertEquals(1, events.get(0).ruleVersion(), "在途窗口沿用旧版本");

        send(env, 10.0, 4000); // 复位
        send(env, 65.0, 4100); // 新窗口按 v2 开窗（65 不再满足 v1 但满足 v2）
        // 重启后新窗口版本仍为 v2，不被丢弃
        TestEnv after = env.restart(logs::add);
        send(after, 66.0, 4200);
        assertEquals(1, after.engine().firedEvents().size(),
                "v2 窗口未到期，且 v1 不重复触发");
        printLogs();
    }

    @Test
    void sameHistoryWithSameRuleVersionIsReproducible(@TempDir Path dir) {
        TestEnv env = base(dir);
        env.link().disconnect();
        AlertRule v1 = env.rules().publish("temperature", 80.0, 1_000L, 0L);
        AlertRule v2 = env.rules().publish("temperature", 70.0, 2_000L, 100_000L);

        for (int i = 0; i < 10; i++) {
            send(env, 75.0 + i, 1000 + i * 600L);
        }
        // 断连下全部记录都在缓冲 pending，取其作为历史样本：
        List<TelemetryRecord> history = env.buffer().pendingRecords();

        List<AlertEvent> run1 = AlertReplay.replay(history, v1);
        List<AlertEvent> run2 = AlertReplay.replay(history, v1);
        List<AlertEvent> runV2 = AlertReplay.replay(history, v2);
        assertEquals(run1, run2, "同一历史+同一版本规则，结论完全一致");
        // v2 阈值更低/窗口更长，结论可不同但确定
        assertEquals(AlertReplay.replay(history, v2), runV2);
        printLogs();
    }

    private void send(TestEnv env, double value, long ts) {
        env.gateway().ingestRegister(new RegisterSample("dev-A", 40001, value, ts));
    }

    private TestEnv base(Path dir) {
        return TestEnv.create(dir, 1000,
                com.github.lechandonga.edgetelemetrygatewayjava.ingest.Normalizer.OutOfOrderPolicy.CLAMP,
                logs::add);
    }

    private void printLogs() {
        logs.forEach(System.out::println);
        logs.clear();
    }
}
