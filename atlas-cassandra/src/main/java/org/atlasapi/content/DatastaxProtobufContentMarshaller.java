package org.atlasapi.content;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.ContentProtos;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

public class DatastaxProtobufContentMarshaller extends ProtobufContentMarshaller<BatchStatement, Iterable<Row>> {

    private static final String TABLE = "content";
    private static final String PRIMARY_KEY_COLUMN = "key";
    private static final String CLUSTERING_KEY_COLUMN = "column1";
    private static final String VALUE_COLUMN = "value";

    protected DatastaxProtobufContentMarshaller(Serializer<Content, ContentProtos.Content> serialiser) {
        super(serialiser);
    }

    @Override
    protected void addColumnToBatch(BatchStatement mutation, Id id, String column, byte[] value) {
        mutation.add(
                update(TABLE)
                        .where(eq(PRIMARY_KEY_COLUMN, id.longValue()))
                        .and(eq(CLUSTERING_KEY_COLUMN, column))
                        .with(set(VALUE_COLUMN, ByteBuffer.wrap(value)))
        );
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
