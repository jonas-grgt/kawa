package io.jonasg.kawa.config;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/// Loads [GatewayConfig] from YAML using Jackson.
public final class ConfigLoader {

    private final YAMLMapper mapper;

    public ConfigLoader() {
        this.mapper = YAMLMapper.builder()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }

    public GatewayConfig load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return mapper.readValue(in, GatewayConfig.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load gateway configuration from " + path, e);
        }
    }

    public GatewayConfig loadFromYaml(String yaml) {
        return mapper.readValue(yaml, GatewayConfig.class);
    }
}
