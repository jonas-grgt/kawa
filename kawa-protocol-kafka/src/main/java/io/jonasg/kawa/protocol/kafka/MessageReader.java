package io.jonasg.kawa.protocol.kafka;

import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.Readable;

/// Reads a message body from a `Readable`. Implementations construct a fresh message
/// instance, call its generated `read(Readable, short)` method and return it.
@FunctionalInterface
public interface MessageReader {

    Message read(
            Readable readable,
            short version
    );
}
