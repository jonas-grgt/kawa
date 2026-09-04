package io.jonasg.kawa.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClientSessionTest {

    @Test
    void writesResponsesInRequestOrderEvenWhenTheyBecomeReadyOutOfOrder() {
        // given
        var channel = new EmbeddedChannel();
        var session = new ClientSession(channel);
        session.requestReceived(1);
        session.requestReceived(2);

        // when
        ByteBuf secondResponse = Unpooled.buffer().writeInt(2);
        session.writeResponse(2, secondResponse);

        // then
        ByteBuf none = channel.readOutbound();
        assertThat(none).isNull();

        // when
        ByteBuf firstResponse = Unpooled.buffer().writeInt(1);
        session.writeResponse(1, firstResponse);

        // then
        ByteBuf firstOut = channel.readOutbound();
        ByteBuf secondOut = channel.readOutbound();
        assertThat(firstOut.readInt()).isEqualTo(1);
        assertThat(secondOut.readInt()).isEqualTo(2);
        firstOut.release();
        secondOut.release();
    }
}
