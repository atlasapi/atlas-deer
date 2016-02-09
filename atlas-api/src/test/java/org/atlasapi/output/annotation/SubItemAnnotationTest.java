package org.atlasapi.output.annotation;

import java.util.Iterator;

import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.QueryContext;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

public class SubItemAnnotationTest {

    @Test
    public void testOrderingOfSubItemsIsBasedReverseLexographicallyOnSortKey() throws Exception {
        SubItemAnnotation anno = new SubItemAnnotation(SubstitutionTableNumberCodec.lowerCaseOnly());

        Series series = new Series();
        EpisodeRef episodeOne = new EpisodeRef(
                Id.valueOf(10l),
                Publisher.METABROADCAST,
                "10",
                DateTime.now()
        );
        EpisodeRef episodeTwo = new EpisodeRef(
                Id.valueOf(20l),
                Publisher.METABROADCAST,
                "20",
                DateTime.now()
        );
        series.setItemRefs(ImmutableList.of(episodeOne, episodeTwo));

        FieldWriter writer = Mockito.mock(FieldWriter.class);

        ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);
        MockHttpServletRequest httpReq = new MockHttpServletRequest("GET", "");
        httpReq.setParameter("sub_item.ordering", "reverse");
        anno.write(series, writer, OutputContext.valueOf(QueryContext.standard(httpReq)));
        verify(writer).writeList(any(), captor.capture(), any());

        Iterator<ItemRef> sortedItemRefIterator = captor.getValue().iterator();
        assertThat(sortedItemRefIterator.next().getSortKey(), is("20"));
        assertThat(sortedItemRefIterator.next().getSortKey(), is("10"));
    }

    @Test
    public void testOrderingOfSubItemsIsBasedLexographicallyOnSortKey() throws Exception {
        SubItemAnnotation anno = new SubItemAnnotation(SubstitutionTableNumberCodec.lowerCaseOnly());

        Series series = new Series();
        EpisodeRef episodeOne = new EpisodeRef(
                Id.valueOf(10l),
                Publisher.METABROADCAST,
                "10",
                DateTime.now()
        );
        EpisodeRef episodeTwo = new EpisodeRef(
                Id.valueOf(20l),
                Publisher.METABROADCAST,
                "20",
                DateTime.now()
        );
        series.setItemRefs(ImmutableList.of(episodeOne, episodeTwo));

        FieldWriter writer = Mockito.mock(FieldWriter.class);

        ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);

        MockHttpServletRequest httpReq = new MockHttpServletRequest("GET", "");
        httpReq.setParameter("sub_items.ordering", "reverse");

        anno.write(series, writer, OutputContext.valueOf(QueryContext.standard(httpReq)));
        verify(writer).writeList(any(), captor.capture(), any());

        Iterator<ItemRef> sortedItemRefIterator = captor.getValue().iterator();
        assertThat(sortedItemRefIterator.next().getSortKey(), is("10"));
        assertThat(sortedItemRefIterator.next().getSortKey(), is("20"));
    }
}