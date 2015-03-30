package org.atlasapi.messaging;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.queue.FailedMessagesStore;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializer;
import kafka.message.MessageAndMetadata;
import org.joda.time.LocalDate;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraFailedMessagesStore implements FailedMessagesStore {

    private static final String FAILED_MESSAGES_TABLE = "failed_messages";

    private static final String TOPIC_KEY = "topic";
    private static final String DAY_KEY = "day";
    private static final String FAILED_TIMESTAMP_KEY = "failed_timestamp";
    private static final String MESSAGE_KEY = "message";
    private static final String STACKTRACE_KEY = "stacktrace";

    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;
    private final Map<Class, MessageSerializer> messageSerializers;

    public CassandraFailedMessagesStore(
            Session session,
            ConsistencyLevel writeConsistency,
            ConsistencyLevel readConsistency,
            Map<Class, MessageSerializer> messageSerializers) {
        this.session = checkNotNull(session);
        this.writeConsistency = checkNotNull(writeConsistency);
        this.readConsistency = checkNotNull(readConsistency);
        this.messageSerializers = ImmutableMap.copyOf(messageSerializers);
    }


    @Override
    public void storeMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, Exception e) {
        session.execute(
                update(FAILED_MESSAGES_TABLE)
                        .where(eq(TOPIC_KEY, messageAndMetadata.topic()))
                        .and(eq(DAY_KEY, new LocalDate().toDateTimeAtStartOfDay().getMillis()))
                        .and(eq(FAILED_TIMESTAMP_KEY, UUIDs.timeBased()))
                        .with(set(MESSAGE_KEY, ByteBuffer.wrap(messageAndMetadata.message())))
                        .and(set(STACKTRACE_KEY, Throwables.getStackTraceAsString(e)))
                        .setConsistencyLevel(writeConsistency)
        );

    }

    @Override
    public <T extends Message> Iterable<T> getFailedMessages(String topic, LocalDate day, Class<T> messageClass) throws MessageDeserializationException {
        ResultSet resultSet = session.execute(
                select().all()
                        .from(FAILED_MESSAGES_TABLE)
                        .where(eq(TOPIC_KEY, topic))
                        .and(eq(DAY_KEY, day.toDateTimeAtStartOfDay().getMillis()))
                        .setConsistencyLevel(readConsistency)
        );
        ImmutableSet.Builder<T> builder = ImmutableSet.builder();
        for (Row row : resultSet) {
            ByteBuffer messageByteBuffer = row.getBytes(MESSAGE_KEY);
            byte[] messageBytes = new byte[messageByteBuffer.remaining()];
            messageByteBuffer.get(messageBytes);
            builder.add((T) messageSerializers.get(messageClass).deserialize(messageBytes));
        }

        return builder.build();
    }
}
