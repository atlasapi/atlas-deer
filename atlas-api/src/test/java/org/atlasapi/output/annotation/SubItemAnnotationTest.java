package org.atlasapi.output.annotation;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.content.Content;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.QueryContext;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SubItemAnnotationTest {

    @Test
    public void testOrderingOfSubItemsIsBasedLexographicallyOnSortKey() throws Exception {
        SubItemAnnotation anno = new SubItemAnnotation(SubstitutionTableNumberCodec.lowerCaseOnly());

        Series series = new Series();
        EpisodeRef episodeOne = new EpisodeRef(Id.valueOf(10l), Publisher.METABROADCAST, "10", DateTime.now());
        EpisodeRef episodeTwo = new EpisodeRef(Id.valueOf(20l), Publisher.METABROADCAST, "20", DateTime.now());
        series.setItemRefs(ImmutableList.of(episodeOne, episodeTwo));

        FieldWriter writer = Mockito.mock(FieldWriter.class);

        ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);
        anno.write(series, writer, OutputContext.valueOf(QueryContext.standard(new MockHttpServletRequest("GET", ""))));
        verify(writer).writeList(any(), captor.capture(), any());

        Iterator<ItemRef> sortedItemRefIterator = captor.getValue().iterator();
        assertThat(sortedItemRefIterator.next().getSortKey(), is("10"));
        assertThat(sortedItemRefIterator.next().getSortKey(), is("20"));
    }
}