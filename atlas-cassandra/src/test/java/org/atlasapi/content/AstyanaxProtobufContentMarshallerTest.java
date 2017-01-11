package org.atlasapi.content;

import java.util.Optional;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AstyanaxProtobufContentMarshallerTest {

    private final ContentMarshaller marshaller = AstyanaxProtobufContentMarshaller.create(
            new ContentSerializer(new ContentSerializationVisitor())
    );

    @Test
    @SuppressWarnings("unchecked")
    public void testMarshallsAndUnmarshallsContentWithoutNullifyingEmptyRepeatedFields() {

        Content content = new Episode();
        content.setId(Id.valueOf(1234));
        content.setPublisher(Publisher.BBC);
        content.setTitle("title");
        content.setActivelyPublished(false);
        content.setGenericDescription(true);

        ColumnListMutation<String> mutation = mock(ColumnListMutation.class);

        marshaller.marshallInto(content.getId(), mutation, content, Optional.empty(), false);

        ArgumentCaptor<String> col = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<byte[]> val = ArgumentCaptor.forClass(byte[].class);

        verify(mutation, times(6)).putColumn(col.capture(), val.capture());

        assertThat(col.getAllValues().size(), is(6));
        assertThat(
                col.getAllValues(),
                hasItems(
                        "IDENTIFICATION",
                        "DESCRIPTION",
                        "SOURCE",
                        "TYPE",
                        "ACTIVELY_PUBLISHED",
                        "GENERIC_DESCRIPTION"
                )
        );

        ImmutableList<Column<String>> columns = ImmutableList.of(
                column(val.getAllValues().get(0)),
                column(val.getAllValues().get(1)),
                column(val.getAllValues().get(2)),
                column(val.getAllValues().get(3)),
                column(val.getAllValues().get(4)),
                column(val.getAllValues().get(5))
        );
        ColumnList<String> cols = mock(ColumnList.class);
        when(cols.iterator())
                .thenReturn(
                        columns.iterator()
                );

        Content unmarshalled = marshaller.unmarshallCols(cols);

        assertThat(unmarshalled.getId(), is(content.getId()));
        assertThat(unmarshalled.getTitle(), is(content.getTitle()));
        assertThat(unmarshalled.isActivelyPublished(), is(false));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMarshallsAndUnmarshallsContent() {

        Content content = new Episode();
        content.setId(Id.valueOf(1234));
        content.setPublisher(Publisher.BBC);
        content.setTitle("title");
        content.setActivelyPublished(false);
        content.setGenericDescription(true);

        ColumnListMutation<String> mutation = mock(ColumnListMutation.class);

        marshaller.marshallInto(content.getId(), mutation, content, Optional.empty(), true);

        ArgumentCaptor<String> col = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<byte[]> val = ArgumentCaptor.forClass(byte[].class);

        verify(mutation, times(14)).putColumn(col.capture(), val.capture());

        assertThat(col.getAllValues().size(), is(14));
        assertThat(
                col.getAllValues(),
                hasItems(
                        "IDENTIFICATION",
                        "DESCRIPTION",
                        "SOURCE",
                        "TYPE",
                        "ACTIVELY_PUBLISHED",
                        "GENERIC_DESCRIPTION"
                )
        );

        ImmutableList<Column<String>> columns = ImmutableList.of(
                column(val.getAllValues().get(0)),
                column(val.getAllValues().get(1)),
                column(val.getAllValues().get(2)),
                column(val.getAllValues().get(3)),
                column(val.getAllValues().get(4)),
                column(val.getAllValues().get(5)),
                column(val.getAllValues().get(6)),
                column(val.getAllValues().get(7)),
                column(val.getAllValues().get(8)),
                column(val.getAllValues().get(9)),
                column(val.getAllValues().get(10)),
                column(val.getAllValues().get(11)),
                column(val.getAllValues().get(12)),
                column(val.getAllValues().get(13))
        );
        ColumnList<String> cols = mock(ColumnList.class);
        when(cols.iterator())
                .thenReturn(
                        columns.iterator()
                );

        Content unmarshalled = marshaller.unmarshallCols(cols);

        assertThat(unmarshalled.getId(), is(content.getId()));
        assertThat(unmarshalled.getTitle(), is(content.getTitle()));
        assertThat(unmarshalled.isActivelyPublished(), is(false));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMarshallsAndUnmarshallsLocations() {

        Item content = new Episode();
        content.setId(Id.valueOf(1234));
        content.setPublisher(Publisher.BBC);
        content.setTitle("title");
        content.setActivelyPublished(false);
        content.setGenericDescription(true);

        Policy policy = new Policy();
        Encoding encoding = new Encoding();
        Location location = new Location();

        location.setPolicy(policy);
        encoding.setAvailableAt(ImmutableSet.of(location));
        //        content.setManifestedAs(ImmutableSet.of(encoding));

        ColumnListMutation<String> mutation = mock(ColumnListMutation.class);

        marshaller.marshallInto(content.getId(), mutation, content, Optional.empty(), true);

        ArgumentCaptor<String> col = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<byte[]> val = ArgumentCaptor.forClass(byte[].class);

        verify(mutation, times(14)).putColumn(col.capture(), val.capture());

        assertThat(col.getAllValues().size(), is(14));
        assertThat(
                col.getAllValues(),
                hasItems(
                        "LOCATIONS",
                        "IDENTIFICATION",
                        "DESCRIPTION",
                        "SOURCE",
                        "TYPE",
                        "ACTIVELY_PUBLISHED",
                        "GENERIC_DESCRIPTION"
                )
        );

        ImmutableList<Column<String>> columns = ImmutableList.of(
                column(val.getAllValues().get(0)),
                column(val.getAllValues().get(1)),
                column(val.getAllValues().get(2)),
                column(val.getAllValues().get(3)),
                column(val.getAllValues().get(4)),
                column(val.getAllValues().get(6)),
                column(val.getAllValues().get(7)),
                column(val.getAllValues().get(8)),
                column(val.getAllValues().get(9)),
                column(val.getAllValues().get(10)),
                column(val.getAllValues().get(11)),
                column(val.getAllValues().get(12)),
                column(val.getAllValues().get(13))
        );
        ColumnList<String> cols = mock(ColumnList.class);
        when(cols.iterator())
                .thenReturn(
                        columns.iterator()
                );

        Content unmarshalled = marshaller.unmarshallCols(cols);

        assertThat(unmarshalled.getId(), is(content.getId()));
        assertThat(unmarshalled.getTitle(), is(content.getTitle()));
        assertThat(unmarshalled.isActivelyPublished(), is(false));

    }

    @SuppressWarnings("unchecked")
    private Column<String> column(byte[] bytes) {
        Column<String> mock = mock(Column.class);
        when(mock.getByteArrayValue()).thenReturn(bytes);
        return mock;
    }

}
