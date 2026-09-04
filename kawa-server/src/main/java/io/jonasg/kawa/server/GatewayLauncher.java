package io.jonasg.kawa.server;

import io.jonasg.kawa.config.ConfigLoader;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.core.Gateway;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public final class GatewayLauncher {

    private GatewayLauncher() {
    }

    public static void main(String[] args) throws Exception {
        Path configPath = parseConfigPath(args);
        GatewayConfig config = new ConfigLoader().load(configPath);
        Gateway gateway = new KafkaGateway(config);

        var shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gateway.stop();
            shutdown.countDown();
        }));

        gateway.start();
        System.out.println("kawa gateway started with config " + configPath);
        shutdown.await();
    }

    private static Path parseConfigPath(String[] args) {
        String path = "config.yaml";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--config") && i + 1 < args.length) {
                path = args[i + 1];
            }
        }
        return Path.of(path);
    }
}
