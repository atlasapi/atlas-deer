package org.atlasapi.messaging;

import java.util.Iterator;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializationException;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;

final class LongMessageSerializer implements MessageSerializer<LongMessage> {

    @Override
    public byte[] serialize(LongMessage message) throws MessageSerializationException {
        return Joiner.on("|").join(message.getMessageId(),
                message.getTimestamp().millis(), message.getValue()).getBytes();
    }

    @Override
    public LongMessage deserialize(byte[] serialized) throws MessageDeserializationException {
        Iterator<String> parts = Splitter.on("|").split(new String(serialized)).iterator();
        return new LongMessage(parts.next(),
                Timestamp.of(Long.valueOf(parts.next())),
                Long.valueOf(parts.next()));
    }

}
