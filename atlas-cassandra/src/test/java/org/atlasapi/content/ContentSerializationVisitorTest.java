package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.intl.Countries;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContentSerializationVisitorTest {

    @Test
    public void testAggregationOfCertificatesFromChildren() throws Exception {
        ContentResolver resolver = mock(ContentResolver.class);

        Episode episodeOne = new Episode(Id.valueOf(20l), Publisher.METABROADCAST);
        episodeOne.setCertificates(ImmutableList.of(new Certificate("PG", Countries.GB)));

        Episode episodeTwo = new Episode(Id.valueOf(30l), Publisher.METABROADCAST);
        episodeTwo.setCertificates(ImmutableList.of(new Certificate("U", Countries.GB)));

        when(resolver.resolveIds(anyList()))
                .thenReturn(itemResults(ImmutableList.of(episodeOne, episodeTwo)));

        ContentSerializationVisitor serializer = new ContentSerializationVisitor(resolver);

        Brand brand = new Brand(Id.valueOf(10l), Publisher.METABROADCAST);

        brand.setCertificates(ImmutableList.of(new Certificate("18", Countries.GB)));

        brand.setItemRefs(
                ImmutableList.of(
                        new EpisodeRef(Id.valueOf(20l), Publisher.METABROADCAST, "", DateTime.now()),
                        new EpisodeRef(Id.valueOf(30l), Publisher.METABROADCAST, "", DateTime.now())
                )
        );

        ContentProtos.Content.Builder serialised = serializer.visit(brand);
        assertThat(serialised.getCertificatesCount(), is(3));
    }

    private ListenableFuture<Resolved<Content>> itemResults(Iterable<Item> items) {
        return Futures.immediateFuture(Resolved.valueOf(ImmutableList.copyOf(items)));
    }

    @Test
    public void testAggregationOfReleaseYearsFromChildren() throws Exception {
        ContentResolver resolver = mock(ContentResolver.class);

        Episode episodeOne = new Episode(Id.valueOf(20l), Publisher.METABROADCAST);
        episodeOne.setYear(1000);

        Episode episodeTwo = new Episode(Id.valueOf(30l), Publisher.METABROADCAST);
        episodeTwo.setYear(1000);

        when(resolver.resolveIds(anyList()))
                .thenReturn(itemResults(ImmutableList.of(episodeOne, episodeTwo)));

        ContentSerializationVisitor serializer = new ContentSerializationVisitor(resolver);

        Brand brand = new Brand(Id.valueOf(10l), Publisher.METABROADCAST);

        brand.setYear(2000);

        brand.setItemRefs(
                ImmutableList.of(
                        new EpisodeRef(Id.valueOf(20l), Publisher.METABROADCAST, "", DateTime.now()),
                        new EpisodeRef(Id.valueOf(30l), Publisher.METABROADCAST, "", DateTime.now())
                )
        );

        ContentProtos.Content.Builder serialised = serializer.visit(brand);
        assertThat(serialised.getReleaseYearsCount(), is(2));
    }
}