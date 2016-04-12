package org.atlasapi.event;

import java.nio.ByteBuffer;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatastaxProtobufEventMarshaller implements EventMarshaller<BatchStatement, Row> {

    private static final String TABLE = "event_v2";
    private static final String PRIMARY_KEY_COLUMN = "event_id";
    private static final String DATA_COLUMN = "data";

    private final Serializer<Event, byte[]> serializer;
    private final PreparedStatement dataUpdate;

    protected DatastaxProtobufEventMarshaller(Serializer<Event, byte[]> serialiser,
            Session session) {
        this.serializer = checkNotNull(serialiser);
        this.dataUpdate = session.prepare(update(TABLE)
                .where(eq(PRIMARY_KEY_COLUMN, bindMarker("id")))
                .with(set(DATA_COLUMN, bindMarker("data"))));
    }

    @Override
    public void marshallInto(Id id, BatchStatement mutation, Event event) {
        byte[] serialized = serializer.serialize(event);
        addDataToBatch(mutation, id, serialized);
    }

    @Override
    public Event unmarshall(Row data) {
        return serializer.deserialize(toByteArrayValues(data));
    }

    protected void addDataToBatch(BatchStatement mutation, Id id, byte[] data) {
        mutation.add(dataUpdate.bind().setLong("id", id.longValue()).setBytes(
                "data",
                ByteBuffer.wrap(data)
        ));
    }

    protected byte[] toByteArrayValues(Row row) {
        ByteBuffer buffer = row.getBytes(DATA_COLUMN);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        return bytes;
    }

}
