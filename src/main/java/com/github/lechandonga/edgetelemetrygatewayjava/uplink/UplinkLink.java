package com.github.lechandonga.edgetelemetrygatewayjava.uplink;

import java.util.concurrent.atomic.AtomicBoolean;

/** 模拟上联链路通断。 */
public class UplinkLink {

    private final AtomicBoolean available = new AtomicBoolean(true);

    public boolean isAvailable() {
        return available.get();
    }

    public void setAvailable(boolean up) {
        available.set(up);
    }

    public void disconnect() {
        setAvailable(false);
    }

    public void connect() {
        setAvailable(true);
    }
}
