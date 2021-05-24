package org.atlasapi.query.v4.schedule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OutputChannelGroupResolverTest {

    private OutputChannelGroupResolver resolver;
    private final ChannelGroupResolver delegateResolver = mock(ChannelGroupResolver.class);

    @Before
    public void setUp() throws Exception {
        resolver = new OutputChannelGroupResolver(delegateResolver);
    }

    private ChannelNumbering channelNumbering(
            long id,
            long channelGroupId,
            @Nullable String channelNumber
    ) {
        return channelNumbering(id, channelGroupId, channelNumber, null, null);
    }

    private ChannelNumbering channelNumberingWithExpiredAvailability(
            long id,
            long channelGroupId,
            @Nullable String channelNumber
    ) {
        return channelNumbering(
                id,
                channelGroupId,
                channelNumber,
                LocalDate.now().minusDays(10),
                LocalDate.now().minusDays(5)
        );
    }

    private ChannelNumbering channelNumberingWithCurrentAvailability(
            long id,
            long channelGroupId,
            @Nullable String channelNumber
    ) {
        return channelNumbering(
                id,
                channelGroupId,
                channelNumber,
                LocalDate.now().minusDays(1),
                LocalDate.now().plusDays(1)
        );
    }

    private ChannelNumbering channelNumbering(
            long id,
            long channelGroupId,
            @Nullable String channelNumber,
            @Nullable LocalDate start,
            @Nullable LocalDate end
    ) {
        ChannelNumbering.Builder channelNumbering = ChannelNumbering.builder(Publisher.METABROADCAST)
                .withChannelId(id)
                .withChannelGroupId(channelGroupId);
        if (channelNumber != null) {
            channelNumbering.withChannelNumber(channelNumber);
        }
        if (start != null) {
            channelNumbering.withStartDate(start);
        }
        if (end != null) {
            channelNumbering.withEndDate(end);
        }
        return channelNumbering.buildChannelNumbering();
    }

    private Platform platform(long id, Iterable<ChannelNumbering> channelNumberings, @Nullable Long channelNumbersFrom) {
        Platform.Builder platform = Platform.builder(Publisher.METABROADCAST)
                .withId(id)
                .withChannels(channelNumberings);
        if (channelNumbersFrom != null) {
            platform.withChannelNumbersFromId(channelNumbersFrom);
        }
        return platform.build();
    }

    private Region region(long id, Iterable<ChannelNumbering> channelNumberings, @Nullable Long channelNumbersFrom) {
        Region.Builder region = Region.builder(Publisher.METABROADCAST)
                .withId(id)
                .withChannels(channelNumberings);
        if (channelNumbersFrom != null) {
            region.withChannelNumbersFromId(channelNumbersFrom);
        }
        return region.build();
    }

    private void setUpResolving(ChannelGroup<?>... channelGroups) {
        List<ChannelGroup<?>> channelGroupList = ImmutableList.copyOf(channelGroups);
        Set<Id> ids = channelGroupList.stream()
                .map(ChannelGroup::getId)
                .collect(MoreCollectors.toImmutableSet());
        when(delegateResolver.resolveIds(ids))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(channelGroupList)));
        when(delegateResolver.resolveIds(ids, false))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(channelGroupList)));
        when(delegateResolver.resolveIds(ids, true))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(channelGroupList)));
        when(delegateResolver.resolveIds(ids, null))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(channelGroupList)));
    }

    private void assertChannelGroupEquals(ChannelGroup<?> expected, ChannelGroup<?> actual) {
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getSource(), actual.getSource());
        assertChannelNumberingsEqual(expected.getChannels(), actual.getChannels());
    }

    private void assertChannelNumberingsEqual(
            Iterable<? extends ChannelGroupMembership> expected,
            Iterable<? extends ChannelGroupMembership> actual
    ) {
        assertEquals(Iterables.size(expected), Iterables.size(actual));
        Iterator<? extends ChannelGroupMembership> expectedIt = expected.iterator();
        Iterator<? extends ChannelGroupMembership> actualIt = actual.iterator();
        while (expectedIt.hasNext() && actualIt.hasNext()) {
            ChannelGroupMembership expectedMembership = expectedIt.next();
            ChannelGroupMembership actualMembership = actualIt.next();
            assertEquals(expectedMembership.getClass(), actualMembership.getClass());
            assertEquals(expectedMembership.getChannel().getId(), actualMembership.getChannel().getId());
            assertEquals(expectedMembership.getChannelGroup().getId(), actualMembership.getChannelGroup().getId());
            assertEquals(expectedMembership.getStartDate(), actualMembership.getStartDate());
            assertEquals(expectedMembership.getEndDate(), actualMembership.getEndDate());
            if (expectedMembership instanceof ChannelNumbering && actualMembership instanceof ChannelNumbering) {
                assertEquals(
                        ((ChannelNumbering) expectedMembership).getChannelNumber(),
                        ((ChannelNumbering) actualMembership).getChannelNumber()
                );
            }
        }
    }

    @Test
    public void testNoChangeForRegularChannelGroup() {
        long groupId = 1;
        long channel1Id = 11;
        long channel2Id = 12;
        long channel3Id = 13;
        String channel1Number = "1";
        String channel2Number = "2";
        String channel3Number = "3";

        List<ChannelNumbering> numberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, channel1Number),
                channelNumberingWithExpiredAvailability(channel2Id, groupId, channel3Number),
                channelNumberingWithCurrentAvailability(channel2Id, groupId, channel2Number),
                channelNumbering(channel3Id, groupId, null)
        );

        Platform platform = platform(groupId, numberings, null);

        setUpResolving(platform);

        Resolved<ChannelGroup<?>> resolved = Futures.getUnchecked(
                resolver.resolveIds(ImmutableSet.of(Id.valueOf(groupId)))
        );

        assertEquals(1, Iterables.size(resolved.getResources()));

        ChannelGroup<?> resolvedGroup = Iterables.getOnlyElement(resolved.getResources());

        assertChannelGroupEquals(platform, resolvedGroup);
    }

    @Test
    public void testChannelNumbersAreAddedForPlatforms() {
        long groupId = 1;
        long baseGroupId = 2;
        long channel1Id = 11;
        long channel2Id = 12;
        long channel3Id = 13;
        long channel4Id = 14;
        String channel1Number = "1";
        String channel2Number = "2";
        String channel3Number = "3";

        List<ChannelNumbering> groupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, null),
                channelNumbering(channel2Id, groupId, null),
                channelNumbering(channel4Id, groupId, null)
        );

        List<ChannelNumbering> baseGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, baseGroupId, channel1Number),
                channelNumbering(channel2Id, baseGroupId, channel2Number),
                channelNumbering(channel3Id, baseGroupId, channel3Number)
        );

        Platform platform = platform(groupId, groupNumberings, baseGroupId);
        Platform basePlatform = platform(baseGroupId, baseGroupNumberings, null);

        setUpResolving(platform);
        setUpResolving(basePlatform);

        Resolved<ChannelGroup<?>> resolved = Futures.getUnchecked(
                resolver.resolveIds(ImmutableSet.of(Id.valueOf(groupId)))
        );

        assertEquals(1, Iterables.size(resolved.getResources()));

        ChannelGroup<?> resolvedGroup = Iterables.getOnlyElement(resolved.getResources());

        List<ChannelNumbering> expectedGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, channel1Number),
                channelNumbering(channel2Id, groupId, channel2Number),
                channelNumbering(channel4Id, groupId, null)
        );

        Platform expected = platform(groupId, expectedGroupNumberings, baseGroupId);

        assertChannelGroupEquals(expected, resolvedGroup);
    }

    @Test
    public void testChannelNumbersAreAddedForRegions() {
        long groupId = 1;
        long baseGroupId = 2;
        long channel1Id = 11;
        long channel2Id = 12;
        long channel3Id = 13;
        long channel4Id = 14;
        String channel1Number = "1";
        String channel2Number = "2";
        String channel3Number = "3";

        List<ChannelNumbering> groupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, null),
                channelNumbering(channel2Id, groupId, null),
                channelNumbering(channel4Id, groupId, null)
        );

        List<ChannelNumbering> baseGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, baseGroupId, channel1Number),
                channelNumbering(channel2Id, baseGroupId, channel2Number),
                channelNumbering(channel3Id, baseGroupId, channel3Number)
        );

        Region region = region(groupId, groupNumberings, baseGroupId);
        Region baseRegion = region(baseGroupId, baseGroupNumberings, null);

        setUpResolving(region);
        setUpResolving(baseRegion);

        Resolved<ChannelGroup<?>> resolved = Futures.getUnchecked(
                resolver.resolveIds(ImmutableSet.of(Id.valueOf(groupId)))
        );

        assertEquals(1, Iterables.size(resolved.getResources()));

        ChannelGroup<?> resolvedGroup = Iterables.getOnlyElement(resolved.getResources());

        List<ChannelNumbering> expectedGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, channel1Number),
                channelNumbering(channel2Id, groupId, channel2Number),
                channelNumbering(channel4Id, groupId, null)
        );

        Region expected = region(groupId, expectedGroupNumberings, baseGroupId);

        assertChannelGroupEquals(expected, resolvedGroup);
    }

    @Test
    public void testChannelNumbersAreAddedForPlatformAndRegion() {
        long groupId = 1;
        long baseGroupId = 2;
        long channel1Id = 11;
        long channel2Id = 12;
        long channel3Id = 13;
        long channel4Id = 14;
        String channel1Number = "1";
        String channel2Number = "2";
        String channel3Number = "3";

        List<ChannelNumbering> groupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, null),
                channelNumbering(channel2Id, groupId, null),
                channelNumbering(channel4Id, groupId, null)
        );

        List<ChannelNumbering> baseGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, baseGroupId, channel1Number),
                channelNumbering(channel2Id, baseGroupId, channel2Number),
                channelNumbering(channel3Id, baseGroupId, channel3Number)
        );

        Platform platform = platform(groupId, groupNumberings, baseGroupId);
        Region baseRegion = region(baseGroupId, baseGroupNumberings, null);

        setUpResolving(platform);
        setUpResolving(baseRegion);

        Resolved<ChannelGroup<?>> resolved = Futures.getUnchecked(
                resolver.resolveIds(ImmutableSet.of(Id.valueOf(groupId)))
        );

        assertEquals(1, Iterables.size(resolved.getResources()));

        ChannelGroup<?> resolvedGroup = Iterables.getOnlyElement(resolved.getResources());

        List<ChannelNumbering> expectedGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, channel1Number),
                channelNumbering(channel2Id, groupId, channel2Number),
                channelNumbering(channel4Id, groupId, null)
        );

        Platform expected = platform(groupId, expectedGroupNumberings, baseGroupId);

        assertChannelGroupEquals(expected, resolvedGroup);
    }

    @Test
    public void testChannelNumbersAreAddedForRegionAndPlatform() {
        long groupId = 1;
        long baseGroupId = 2;
        long channel1Id = 11;
        long channel2Id = 12;
        long channel3Id = 13;
        long channel4Id = 14;
        String channel1Number = "1";
        String channel2Number = "2";
        String channel3Number = "3";

        List<ChannelNumbering> groupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, null),
                channelNumbering(channel2Id, groupId, null),
                channelNumbering(channel4Id, groupId, null)
        );

        List<ChannelNumbering> baseGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, baseGroupId, channel1Number),
                channelNumbering(channel2Id, baseGroupId, channel2Number),
                channelNumbering(channel3Id, baseGroupId, channel3Number)
        );

        Region region = region(groupId, groupNumberings, baseGroupId);
        Platform basePlatform = platform(baseGroupId, baseGroupNumberings, null);

        setUpResolving(region);
        setUpResolving(basePlatform);

        Resolved<ChannelGroup<?>> resolved = Futures.getUnchecked(
                resolver.resolveIds(ImmutableSet.of(Id.valueOf(groupId)))
        );

        assertEquals(1, Iterables.size(resolved.getResources()));

        ChannelGroup<?> resolvedGroup = Iterables.getOnlyElement(resolved.getResources());

        List<ChannelNumbering> expectedGroupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, channel1Number),
                channelNumbering(channel2Id, groupId, channel2Number),
                channelNumbering(channel4Id, groupId, null)
        );

        Region expected = region(groupId, expectedGroupNumberings, baseGroupId);

        assertChannelGroupEquals(expected, resolvedGroup);
    }

    @Test
    public void testExistingMembershipIsReplacedWhenChannelNumbersFromAnotherGroup() {
        long groupId = 1;
        long baseGroupId = 2;
        long channel1Id = 11;
        long channel2Id = 12;
        long channel3Id = 13;
        long channel4Id = 14;
        String channel1Number = "1";
        String channel2Number = "2";
        String channel3Number = "3";
        String channel4Number = "4";

        List<ChannelNumbering> groupNumberings = ImmutableList.of(
                channelNumbering(channel1Id, groupId, null),
                channelNumberingWithExpiredAvailability(channel1Id, groupId, channel3Number),
                channelNumbering(channel2Id, groupId, null),
                channelNumberingWithExpiredAvailability(channel2Id, groupId, null),
                channelNumberingWithExpiredAvailability(channel4Id, groupId, channel2Number),
                channelNumbering(channel4Id, groupId, channel4Number)
        );

        List<ChannelNumbering> baseGroupNumberings = ImmutableList.of(
                channelNumberingWithCurrentAvailability(channel1Id, baseGroupId, channel1Number),
                channelNumbering(channel2Id, baseGroupId, channel2Number),
                channelNumbering(channel3Id, baseGroupId, channel3Number)
        );

        Platform platform = platform(groupId, groupNumberings, baseGroupId);
        Platform basePlatform = platform(baseGroupId, baseGroupNumberings, null);

        setUpResolving(platform);
        setUpResolving(basePlatform);

        Resolved<ChannelGroup<?>> resolved = Futures.getUnchecked(
                resolver.resolveIds(ImmutableSet.of(Id.valueOf(groupId)))
        );

        assertEquals(1, Iterables.size(resolved.getResources()));

        ChannelGroup<?> resolvedGroup = Iterables.getOnlyElement(resolved.getResources());

        List<ChannelNumbering> expectedGroupNumberings = ImmutableList.of(
                channelNumberingWithCurrentAvailability(channel1Id, groupId, channel1Number),
                channelNumbering(channel2Id, groupId, channel2Number),
                channelNumbering(channel4Id, groupId, null)
        );

        Platform expected = platform(groupId, expectedGroupNumberings, baseGroupId);

        assertChannelGroupEquals(expected, resolvedGroup);
    }
}