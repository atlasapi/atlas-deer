package org.atlasapi.content;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.ContentProtos;

import javax.annotation.Nullable;

public class AstyanaxProtobufContentMarshaller extends ProtobufContentMarshaller<ColumnListMutation<String>,ColumnList<String>> {

    public AstyanaxProtobufContentMarshaller(Serializer<Content, ContentProtos.Content> serialiser) {
        super(serialiser);
    }

    @Override
    protected void addColumnToBatch(ColumnListMutation<String> mutation, Id id, String column, byte[] value) {
        mutation.putColumn(column, value);
    }

    @Override
    protected Iterable<byte[]> toByteArrayValues(ColumnList<String> columns) {
        return Iterables.transform(
                columns,
                new Function<Column<String>, byte[]>() {
                    @Nullable
                    @Override
                    public byte[] apply(Column<String> column) {
                        return column.getByteArrayValue();
                    }
                }
        );
    }
}
