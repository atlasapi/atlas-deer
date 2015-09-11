package org.atlasapi.event;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Batch;

public class DatastaxProtobufEventMarshaller implements EventMarshaller<Batch, Row> {

    private static final String TABLE = "event";
    private static final String PRIMARY_KEY_COLUMN = "event_id";
    private static final String DATE_COLUMN = "data";

    private final Serializer<Event, byte[]> serializer;

    protected DatastaxProtobufEventMarshaller(Serializer<Event, byte[]> serialiser) {
        this.serializer = checkNotNull(serialiser);
    }

    @Override
    public void marshallInto(Id id, Batch mutation, Event event) {
        byte[] serialized = serializer.serialize(event);
        addDataToBatch(mutation, id, serialized);
    }

    @Override
    public Event unmarshall(Row data) {
        return serializer.deserialize(toByteArrayValues(data));
    }

    protected void addDataToBatch(Batch mutation, Id id, byte[] data) {
        mutation.add(
                update(TABLE)
                        .where(eq(PRIMARY_KEY_COLUMN, id.longValue()))
                        .with(set(DATE_COLUMN, ByteBuffer.wrap(data)))
        );
    }

    protected byte[] toByteArrayValues(Row row) {
        ByteBuffer buffer = row.getBytes(DATE_COLUMN);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        return bytes;
    }
}
