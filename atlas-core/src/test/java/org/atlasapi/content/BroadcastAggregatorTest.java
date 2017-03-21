package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BroadcastAggregatorTest {

    private final ChannelResolver channelResolver = mock(ChannelResolver.class);
    private final Channel variantRefParent = mock(Channel.class);

    private final BroadcastAggregator broadcastAggregator = BroadcastAggregator.create(channelResolver);
    private ListenableFuture<Resolved<Channel>> resolvedListenableFuture1;
    private ListenableFuture<Resolved<Channel>> resolvedListenableFuture2;

    private Set<Id> includedVariantIds;
    private Set<Id> excludedVariantIds;

    @Before
    public void setUp() throws Exception {

        resolvedListenableFuture1 = resolvedChannel(444L);
        resolvedListenableFuture2 = resolvedChannel(555L);

        when(channelResolver.resolveIds(ImmutableList.of(Id.valueOf(444L))))
                .thenReturn(resolvedListenableFuture1);
        when(channelResolver.resolveIds(ImmutableList.of(Id.valueOf(555L))))
                .thenReturn(resolvedListenableFuture2);

        when(variantRefParent.getTitle()).thenReturn("Parent");
        when(variantRefParent.getVariations()).thenReturn(ImmutableSet.of(
                new ChannelRef(Id.valueOf(111L), Publisher.METABROADCAST),
                new ChannelRef(Id.valueOf(222L), Publisher.METABROADCAST),
                new ChannelRef(Id.valueOf(333L), Publisher.METABROADCAST),
                new ChannelRef(Id.valueOf(444L), Publisher.METABROADCAST),
                new ChannelRef(Id.valueOf(555L), Publisher.METABROADCAST)
        ));

        includedVariantIds = ImmutableSet.of(Id.valueOf(111L), Id.valueOf(222L), Id.valueOf(333L));
        excludedVariantIds = ImmutableSet.of(Id.valueOf(444L), Id.valueOf(555L));

    }

    public void aggregatedBroadcastUsesOwnChannelIfNoParent() throws Exception {
        //TODO: write this test
    }

    @Test
    public void singleBroadcastIsReturnedNormally() throws Exception {

        Broadcast broadcast = getFutureBroadcast(444L, 1, 2);

        Set<ResolvedBroadcast> resolvedBroadcasts = broadcastAggregator.aggregateBroadcasts(
                ImmutableSet.of(broadcast),
                Optional.empty(),
                ImmutableList.of()
        );

        assertThat(Iterables.getOnlyElement(resolvedBroadcasts).getBroadcast(), is(broadcast));

    }

    @Test
    public void broadcastOnSameChannelAtDifferentTimesDoNotAggregate() throws Exception {
        Broadcast firstBroadcast = getFutureBroadcast(444L, 1, 2);
        Broadcast secondBroadcast = getFutureBroadcast(444L, 5, 6);

        Set<ResolvedBroadcast> resolvedBroadcasts = broadcastAggregator.aggregateBroadcasts(
                ImmutableSet.of(firstBroadcast, secondBroadcast),
                Optional.empty(),
                ImmutableList.of()
        );

        assertThat(resolvedBroadcasts.size(), is(2));

    }

    @Test
    public void downweighChannelBroadcastIsNotFirst() throws Exception {

        Broadcast downweighedBroadcast1 = new Broadcast(Id.valueOf(333L), DateTime.now().plusHours(1), DateTime.now().plusHours(2));
        Broadcast normalBroadcast1 = new Broadcast(Id.valueOf(111L), DateTime.now().plusHours(1), DateTime.now().plusHours(2));
        Broadcast normalBroadcast2 = new Broadcast(Id.valueOf(222L), DateTime.now().plusHours(1), DateTime.now().plusHours(2));


        Collection<ResolvedBroadcast> resolvedBroadcasts = ImmutableSet.of(
                ResolvedBroadcast.create(downweighedBroadcast1, ResolvedChannel.builder(getMockChannel(333L)).build()),
                ResolvedBroadcast.create(normalBroadcast1, ResolvedChannel.builder(getMockChannel(111L)).build()),
                ResolvedBroadcast.create(normalBroadcast2, ResolvedChannel.builder(getMockChannel(222L)).build())
        );

        assertThat(resolvedBroadcasts.iterator().next().getBroadcast(), is(downweighedBroadcast1));

        Set<ResolvedBroadcast> sortedBroadcasts = broadcastAggregator.sortByDownweighChannelIds(
                ImmutableList.of(Id.valueOf(333L)),
                resolvedBroadcasts
        );

        assertFalse(sortedBroadcasts.iterator().next().getBroadcast().equals(downweighedBroadcast1));
        assertTrue(sortedBroadcasts.stream()
                .map(ResolvedBroadcast::getBroadcast)
                .anyMatch(broadcast -> broadcast.equals(downweighedBroadcast1))
        );

    }

    @Test
    public void aggregatedBroadcastsAreSortedByDateTime() throws Exception {
        ResolvedChannel channel = mock(ResolvedChannel.class);

        Set<ResolvedBroadcast> broadcasts = ImmutableSet.of(
                ResolvedBroadcast.create(getFutureBroadcast(40, 7, 8), channel),
                ResolvedBroadcast.create(getFutureBroadcast(30, 5, 6), channel),
                ResolvedBroadcast.create(getFutureBroadcast(10, 1, 2), channel),
                ResolvedBroadcast.create(getFutureBroadcast(20, 3, 4), channel)
        );

        ImmutableMultimap<DateTime, ResolvedBroadcast> broadcastMultimap = broadcasts.stream()
                .collect(MoreCollectors.toImmutableListMultiMap(
                        resolvedBroadcast -> resolvedBroadcast.getBroadcast().getTransmissionTime(),
                        resolvedBroadcast -> resolvedBroadcast
                ));

        ImmutableList<ResolvedBroadcast> sortedBroadcasts = ImmutableList.copyOf(
                broadcastAggregator.sortBroadcastsByDateTime(broadcastMultimap)
        );

        assertThat(
                sortedBroadcasts.get(0).getBroadcast().getChannelId().longValue(),
                is(10L)
        );
        assertThat(
                sortedBroadcasts.get(1).getBroadcast().getChannelId().longValue(),
                is(20L)
        );
        assertThat(
                sortedBroadcasts.get(2).getBroadcast().getChannelId().longValue(),
                is(30L)
        );
        assertThat(
                sortedBroadcasts.get(3).getBroadcast().getChannelId().longValue(),
                is(40L)
        );

    }

    @Test
    public void broadcastsAreRemovedIfNotOnPlatform() throws Exception {

        Broadcast broadcastOnPlatform = getFutureBroadcast(111L, 1, 2);
        Broadcast broadcastNotOnPlatform = getFutureBroadcast(333L, 1, 2);

        Iterable<Broadcast> unfilteredBroadcasts = ImmutableList.of(broadcastOnPlatform, broadcastNotOnPlatform);

        Platform platform = new Platform(
                Id.valueOf(80085L),
                Publisher.METABROADCAST,
                ImmutableSet.of(
                        getChannelNumbering(111L),
                        getChannelNumbering(222L)
                ),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of()
        );

        Set<Broadcast> filteredBroadcasts = broadcastAggregator.removeBroadcastsNotOnPlatform(
                unfilteredBroadcasts,
                platform
        );

        assertThat(filteredBroadcasts.size(), is(1));
        assertThat(Iterables.getOnlyElement(filteredBroadcasts), is(broadcastOnPlatform));
    }

    @Test
    public void onlyGetsAllIncludedVariantRefs() throws Exception {

        Map<Id, String> includedChildMap = ImmutableMap.of(
                Id.valueOf(111L), "Parent ref111",
                Id.valueOf(222L), "Parent ref222",
                Id.valueOf(333L), "Parent ref333"
        );

        List<ChannelVariantRef> includedRefs = broadcastAggregator.getIncludedVariantRefs(
                variantRefParent,
                includedChildMap.entrySet()
        );

        assertThat(includedRefs.size(), is(3));
        assertThat(
                includedRefs.stream().allMatch(channelVariantRef ->
                        includedVariantIds.contains(channelVariantRef.getId())
                ),
                is(true));
    }

    @Test
    public void GetsAllExcludedVariantRefsWithNoPlatform() throws Exception {

        List<ChannelVariantRef> excludedRefs = broadcastAggregator.resolveExcludedVariantRefs(
                variantRefParent,
                includedVariantIds,
                Optional.empty()
        );

        assertThat(excludedRefs.size(), is(2));
        assertTrue(
                excludedRefs.stream().allMatch(channelVariantRef ->
                        excludedVariantIds.contains(channelVariantRef.getId())
                )
        );
    }

    @Test
    public void onlyGetsExcludedVariantRefsFromPlatformWhenProvided() throws Exception {

        Platform platform = mock(Platform.class);
        ChannelNumbering channelNumbering = mock(ChannelNumbering.class);
        ChannelRef channelRef = new ChannelRef(Id.valueOf(444L), Publisher.METABROADCAST);

        when(channelNumbering.getChannel()).thenReturn(channelRef);
        when(platform.getChannels()).thenReturn(ImmutableList.of(channelNumbering));

        List<ChannelVariantRef> excludedRefs = broadcastAggregator.resolveExcludedVariantRefs(
                variantRefParent,
                includedVariantIds,
                Optional.of(platform)
        );

        assertThat(excludedRefs.size(), is(1));
        assertThat(excludedRefs.get(0).getId(), is(channelRef.getId()));
    }

    @Test
    public void aggregatedBroadcastDoesNotHaveExcludedVariantsFromAnotherPlatform() throws Exception {

    }

    @Test
    public void childTitlesAreParsedCorrectlyFromParent() throws Exception {
        String parent = "BBC One";

        assertThat(
                broadcastAggregator.parseChildTitle(parent, "BBC One London"),
                is("London")
        );
        assertThat(
                broadcastAggregator.parseChildTitle(parent, "BBC One London HD"),
                is("London HD")
        );

        assertThat(
                broadcastAggregator.parseChildTitle(parent, "BBC One"),
                is("BBC One")
        );

        assertThat(
                broadcastAggregator.parseChildTitle(parent, "BBC One HD"),
                is("BBC One HD")
        );

        assertThat(
                broadcastAggregator.parseChildTitle(parent, "BBC One HD +1"),
                is("BBC One HD +1")
        );
    }

    private ChannelNumbering getChannelNumbering(long id) {
        return ChannelNumbering.builder(Publisher.METABROADCAST)
                .withChannelId(id)
                .withChannelGroupId(id + 1L)
                .buildChannelNumbering();
    }

    private ListenableFuture<Resolved<Channel>> resolvedChannel(long id) throws Exception {

        ListenableFuture<Resolved<Channel>> listenableFuture = mock(ListenableFuture.class);
        Channel channel = getMockChannel(id);

        when(listenableFuture.get()).thenReturn(Resolved.valueOf(ImmutableList.of(channel)));

        return listenableFuture;
    }

    private Channel getMockChannel(long id) {
        Channel channel = mock(Channel.class);

        String title = "Parent ref" + id;

        when(channel.getTitle()).thenReturn(title);
        when(channel.getId()).thenReturn(Id.valueOf(id));

        return channel;
    }

    private Broadcast getFutureBroadcast(long id, int startNowPlusHours, int endNowPlusHours) {
        return new Broadcast(
                Id.valueOf(id),
                DateTime.now().plusHours(startNowPlusHours),
                DateTime.now().plusHours(endNowPlusHours)
        );
    }

}
