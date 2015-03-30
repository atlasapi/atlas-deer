package org.atlasapi.messaging;


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializationException;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import kafka.message.MessageAndMetadata;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CassandraFailedMessagesStoreIT {

    private static TestCassandraPersistenceModule MODULE;

    private CassandraFailedMessagesStore objectUnderTest;

    private LongMessageSerializer messageSerializer = new LongMessageSerializer();

    @BeforeClass
    public static void setUpCassandra() throws TimeoutException {
        MODULE = new TestCassandraPersistenceModule() {
            @Override
            protected void createTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
                session.execute("CREATE TABLE atlas_testing.failed_messages (topic text, day timestamp, failed_timestamp timeuuid, message blob, stacktrace text, PRIMARY KEY ((topic, day), failed_timestamp));");
            }

            @Override
            protected void clearTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
                session.execute(String.format("TRUNCATE failed_messages"));
            }
        };
        MODULE.startAsync().awaitRunning(1, TimeUnit.MINUTES);
    }

    @Before
    public void setUp() {
        objectUnderTest = new CassandraFailedMessagesStore(
                MODULE.getCassandraSession(),
                ConsistencyLevel.ALL,
                ConsistencyLevel.ALL,
                ImmutableMap.<Class, MessageSerializer>of(LongMessage.class, messageSerializer)
        );
    }

    @After
    public void tearDown() throws ConnectionException {
        MODULE.reset();
    }

    @Test
    public void testSaveMessage() {
        byte[] messagePayload = new byte[10];
        new Random().nextBytes(messagePayload);
        MessageAndMetadata<byte[], byte[]> messageAndMetadata = mock(MessageAndMetadata.class);
        Exception e = new Exception("testException");
        when(messageAndMetadata.topic()).thenReturn("testTopic");
        when(messageAndMetadata.message()).thenReturn(messagePayload);
        objectUnderTest.storeMessage(messageAndMetadata, e);
        ResultSet resultSet = MODULE.getCassandraSession().execute(
                select().all()
                        .from("failed_messages")
                        .where(eq("topic", "testTopic"))
                        .and(eq("day", new LocalDate().toDateTimeAtStartOfDay().getMillis()))
                        .setConsistencyLevel(ConsistencyLevel.ALL)
        );

        Row result = resultSet.one();
        ByteBuffer messageByteBuffer = result.getBytes("message");
        byte[] messageBytes = new byte[messageByteBuffer.remaining()];
        messageByteBuffer.get(messageBytes);
        assertThat(result.getString("stacktrace"), startsWith("java.lang.Exception: testException"));
        assertThat(messageBytes, is(messagePayload));
    }


    @Test
    public void testReadMessage() throws MessageSerializationException, MessageDeserializationException {
        LongMessage m1 = new LongMessage("mId", Timestamp.of(1L), 1L);
        byte[] messagePayload = messageSerializer.serialize(m1);
        LocalDate day = new LocalDate();
        MODULE.getCassandraSession().execute(
                update("failed_messages")
                        .where((eq("topic", "testTopic")))
                        .and(eq("day", day.toDateTimeAtStartOfDay().getMillis()))
                        .and(eq("failed_timestamp", UUIDs.timeBased()))
                        .with(set("message", ByteBuffer.wrap(messagePayload)))
                        .and(set("stacktrace", "some stacktrace"))
                        .setConsistencyLevel(ConsistencyLevel.ALL)
        );

        Iterable<LongMessage> failedMessages = objectUnderTest.getFailedMessages("testTopic", day, LongMessage.class);

        assertThat(Iterables.getOnlyElement(failedMessages), is(m1));
    }

    @Test
    public void testWriteReadMessages() throws MessageSerializationException, MessageDeserializationException {
        LongMessage m1 = new LongMessage("mId1", Timestamp.of(1L), 1L);
        byte[] messagePayload1 = messageSerializer.serialize(m1);

        LongMessage m2 = new LongMessage("mId2", Timestamp.of(2L), 2L);
        byte[] messagePayload2 = messageSerializer.serialize(m2);

        LongMessage m3 = new LongMessage("mId3", Timestamp.of(3L), 3L);
        byte[] messagePayload3 = messageSerializer.serialize(m3);

        MessageAndMetadata<byte[], byte[]> messageAndMetadata1 = mock(MessageAndMetadata.class);
        Exception e1 = new Exception("testException1");
        when(messageAndMetadata1.topic()).thenReturn("testTopic");
        when(messageAndMetadata1.message()).thenReturn(messagePayload1);

        MessageAndMetadata<byte[], byte[]> messageAndMetadata2 = mock(MessageAndMetadata.class);
        Exception e2 = new Exception("testException2");
        when(messageAndMetadata2.topic()).thenReturn("testTopic");
        when(messageAndMetadata2.message()).thenReturn(messagePayload2);

        MessageAndMetadata<byte[], byte[]> messageAndMetadata3 = mock(MessageAndMetadata.class);
        Exception e3 = new Exception("testException3");
        when(messageAndMetadata3.topic()).thenReturn("testTopic");
        when(messageAndMetadata3.message()).thenReturn(messagePayload3);

        objectUnderTest.storeMessage(messageAndMetadata1, e1);
        objectUnderTest.storeMessage(messageAndMetadata2, e2);
        objectUnderTest.storeMessage(messageAndMetadata3, e3);

        Iterable<LongMessage> failedMessages = objectUnderTest.getFailedMessages(
                "testTopic",
                new LocalDate(),
                LongMessage.class
        );

        assertThat(failedMessages, containsInAnyOrder(m1, m2, m3));
    }
}