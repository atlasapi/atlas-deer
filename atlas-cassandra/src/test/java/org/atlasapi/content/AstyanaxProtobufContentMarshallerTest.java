package org.atlasapi.content;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;

public class AstyanaxProtobufContentMarshallerTest {

    private final ContentMarshaller marshaller = new AstyanaxProtobufContentMarshaller(new ContentSerializer(new ContentSerializationVisitor(new NoOpContentResolver())));

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
        
        marshaller.marshallInto(content.getId(), mutation, content);
        
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

    @SuppressWarnings("unchecked")
    private Column<String> column(byte[] bytes) {
        Column<String> mock = mock(Column.class);
        when(mock.getByteArrayValue()).thenReturn(bytes);
        return mock;
    }

}
