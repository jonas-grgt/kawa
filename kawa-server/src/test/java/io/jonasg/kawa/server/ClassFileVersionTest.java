package io.jonasg.kawa.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class ClassFileVersionTest {

    private static final int JAVA_26_CLASS_FILE_MAJOR_VERSION = 70;

    @Test
    void classesAreEmittedWithJava26ClassFileVersion() throws IOException {
        // given the class file of this very class as it lands in the built jar
        String classResource = getClass().getName().replace('.', '/') + ".class";

        // when reading its header
        int majorVersion;
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(classResource)) {
            assertThat(in).isNotNull();
            byte[] header = in.readNBytes(8);
            assertThat(header).hasSize(8);
            majorVersion = ((header[6] & 0xFF) << 8) | (header[7] & 0xFF);
        }

        // then the bytecode targets Java 26 so the runtime image can require a matching JRE
        assertThat(majorVersion)
                .as("class file major version of %s", getClass().getName())
                .isEqualTo(JAVA_26_CLASS_FILE_MAJOR_VERSION);
    }
}
