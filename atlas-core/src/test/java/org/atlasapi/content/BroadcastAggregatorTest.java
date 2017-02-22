package org.atlasapi.content;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.ChannelRef;
import org.atlasapi.channel.ChannelResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
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

    @Test
    public void downweighChannelIsNotFirst() throws Exception {
        //TODO: write this test
    }

    @Test
    public void broadcastsAreRemovedIfNotOnPlatform() throws Exception {

        Broadcast validBroadcast = new Broadcast(Id.valueOf(111L), DateTime.now().plusHours(1), DateTime.now().plusHours(2));
        Broadcast invalidBroadcast = new Broadcast(Id.valueOf(333L), DateTime.now().plusHours(1), DateTime.now().plusHours(2));

        Iterable<Broadcast> unfilteredBroadcasts = ImmutableList.of(validBroadcast, invalidBroadcast);

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

        Set<Broadcast> filteredBroadcasts = broadcastAggregator.removeBroadcastsNotOnPlatform(unfilteredBroadcasts, platform);

        assertThat(filteredBroadcasts.size(), is(1));
        assertThat(Iterables.getOnlyElement(filteredBroadcasts), is(validBroadcast));
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
    public void onlyGetsAllExcludedVariantRefs() throws Exception {

        List<ChannelVariantRef> excludedRefs = broadcastAggregator.resolveExcludedVariantRefs(
                variantRefParent,
                excludedVariantIds
        );

        assertThat(excludedRefs.size(), is(2));
        assertThat(
                excludedRefs.stream().allMatch(channelVariantRef ->
                        excludedVariantIds.contains(channelVariantRef.getId())
                ),
                is(true)
        );
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
    }

    private ChannelNumbering getChannelNumbering(long id) {
        return ChannelNumbering.builder(Publisher.METABROADCAST)
                .withChannelId(id)
                .withChannelGroupId(id + 1L)
                .buildChannelNumbering();
    }

    private ListenableFuture<Resolved<Channel>> resolvedChannel(long id) throws Exception {

        String title = "Parent ref" + id;

        ListenableFuture<Resolved<Channel>> listenableFuture = mock(ListenableFuture.class);
        Channel channel = mock(Channel.class);

        when(channel.getTitle()).thenReturn(title);
        when(channel.getId()).thenReturn(Id.valueOf(id));

        when(listenableFuture.get()).thenReturn(Resolved.valueOf(ImmutableList.of(channel)));

        return listenableFuture;
    }

}
