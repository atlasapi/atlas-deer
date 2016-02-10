package org.atlasapi.content;

import java.nio.ByteBuffer;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatastaxProtobufContentMarshaller
        extends ProtobufContentMarshaller<BatchStatement, Iterable<Row>> {

    private static final String TABLE = "content";
    private static final String PRIMARY_KEY_COLUMN = "key";
    private static final String CLUSTERING_KEY_COLUMN = "column1";
    private static final String VALUE_COLUMN = "value";

    private final Session session;
    private final PreparedStatement updateStatement;

    protected DatastaxProtobufContentMarshaller(
            Serializer<Content, ContentProtos.Content> serialiser, Session session) {
        super(serialiser);
        this.session = checkNotNull(session);

        this.updateStatement = session.prepare(update(TABLE)
                .where(eq(PRIMARY_KEY_COLUMN, bindMarker("key")))
                .and(eq(CLUSTERING_KEY_COLUMN, bindMarker("clustering")))
                .with(set(VALUE_COLUMN, bindMarker("value")))
        );
    }

    @Override
    protected void addColumnToBatch(BatchStatement mutation, Id id, String column, byte[] value) {
        mutation.add(updateStatement.bind()
                .setLong("key", id.longValue())
                .setString("clustering", column)
                .setBytes("value", ByteBuffer.wrap(value)));
    }

    @Override
    protected Iterable<byte[]> toByteArrayValues(Iterable<Row> rows) {
        ImmutableSet.Builder<byte[]> result = ImmutableSet.builder();

        for (Row row : rows) {
            ByteBuffer buffer = row.getBytes(VALUE_COLUMN);
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            result.add(bytes);
        }
        return result.build();
    }

}
