package org.atlasapi.query.v4.schedule;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.NumberedChannelGroup;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.ChannelGroupQuery;
import org.joda.time.LocalDate;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A ChannelGroupResolver which performs additional operations on a channel group after resolving when used in API
 * output. For example to add channel numbers to channel groups with the "channelNumbersFrom" field set.
 */
public class OutputChannelGroupResolver implements ChannelGroupResolver {

    private final ChannelGroupResolver delegate;

    public OutputChannelGroupResolver(ChannelGroupResolver delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> allChannelGroups() {
        // Refresh the cache since there's otherwise no way to bypass the cache
        return handleResult(delegate.allChannelGroups(), true);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids) {
        return handleResult(delegate.resolveIds(ids), false);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids, boolean refreshCache) {
        return handleResult(delegate.resolveIds(ids, refreshCache), refreshCache);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveChannelGroups(ChannelGroupQuery channelGroupQuery) {
        // Refresh the cache since there's otherwise no way to bypass the cache
        return handleResult(delegate.resolveChannelGroups(channelGroupQuery), true);
    }

    private ListenableFuture<Resolved<ChannelGroup<?>>> handleResult(
            ListenableFuture<Resolved<ChannelGroup<?>>> delegateResult,
            boolean refreshCacheForAdditionalIds
    ) {
        return Futures.transformAsync(delegateResult, result -> {
            ImmutableMap.Builder<Id, ChannelGroup<?>> channelGroupByIdBuilder = ImmutableMap.builder();
            ImmutableList.Builder<ChannelGroup<?>> channelGroupsBuilder = ImmutableList.builder();
            ImmutableList.Builder<NumberedChannelGroup> channelGroupsToFillChannelNumbersBuilder =
                    ImmutableList.builder();
            for (ChannelGroup<?> channelGroup : result.getResources()) {
                channelGroupByIdBuilder.put(channelGroup.getId(), channelGroup);
                channelGroupsBuilder.add(channelGroup);
                if (channelGroup instanceof NumberedChannelGroup) {
                    NumberedChannelGroup numberedChannelGroup = (NumberedChannelGroup) channelGroup;
                    if (numberedChannelGroup.getChannelNumbersFrom().isPresent()) {
                        channelGroupsToFillChannelNumbersBuilder.add(numberedChannelGroup);
                    }
                }
            }
            final ImmutableMap<Id, ChannelGroup<?>> channelGroupById = channelGroupByIdBuilder.build();
            final ImmutableList<ChannelGroup<?>> channelGroups = channelGroupsBuilder.build();
            final ImmutableList<NumberedChannelGroup> channelGroupsToFillChannelNumbers =
                    channelGroupsToFillChannelNumbersBuilder.build();

            ImmutableSet<Id> additionalChannelGroupsToResolve = channelGroupsToFillChannelNumbers.stream()
                    .map(NumberedChannelGroup::getChannelNumbersFrom)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(ChannelGroupRef::getId)
                    .filter(id -> !channelGroupById.containsKey(id))
                    .collect(MoreCollectors.toImmutableSet());

            ListenableFuture<Resolved<ChannelGroup<?>>> additionalChannelGroupsFuture =
                    additionalChannelGroupsToResolve.isEmpty()
                            ? Futures.immediateFuture(Resolved.empty())
                            : delegate.resolveIds(additionalChannelGroupsToResolve, refreshCacheForAdditionalIds);

            return Futures.transform(additionalChannelGroupsFuture,
                    (Function<Resolved<ChannelGroup<?>>, Resolved<ChannelGroup<?>>>) additionalChannelGroups -> {
                        ImmutableMap.Builder<Id, ChannelGroup<?>> additionalChannelGroupByIdMapBuilder =
                                ImmutableMap.builder();
                        additionalChannelGroupByIdMapBuilder.putAll(channelGroupById);
                        for (ChannelGroup<?> channelGroup : additionalChannelGroups.getResources()) {
                            additionalChannelGroupByIdMapBuilder.put(channelGroup.getId(), channelGroup);
                        }
                        ImmutableMap<Id, ChannelGroup<?>> additionalChannelGroupByIdMap =
                                additionalChannelGroupByIdMapBuilder.build();

                        LocalDate now = LocalDate.now();

                        ImmutableMap.Builder<Id, ChannelGroup<ChannelNumbering>>
                                channelGroupWithAddedNumbersByIdMapBuilder = ImmutableMap.builder();

                        for (NumberedChannelGroup channelGroupToFillChannelNumbers : channelGroupsToFillChannelNumbers) {
                            ChannelGroup<ChannelNumbering> channelGroupWithAddedNumbers =
                                    addChannelNumbersToChannelGroup(
                                            channelGroupToFillChannelNumbers,
                                            additionalChannelGroupByIdMap,
                                            now
                                    );
                            channelGroupWithAddedNumbersByIdMapBuilder.put(
                                    channelGroupToFillChannelNumbers.getId(),
                                    channelGroupWithAddedNumbers
                            );
                        }
                        ImmutableMap<Id, ChannelGroup<ChannelNumbering>> channelGroupWithAddedNumbersByIdMap =
                                channelGroupWithAddedNumbersByIdMapBuilder.build();

                        return Resolved.valueOf(
                                channelGroups.stream()
                                        .map(channelGroup -> {
                                            ChannelGroup<ChannelNumbering> channelGroupWithAddedNumbers =
                                                    channelGroupWithAddedNumbersByIdMap.get(channelGroup.getId());
                                            return channelGroupWithAddedNumbers != null
                                                    ? channelGroupWithAddedNumbers
                                                    : channelGroup;
                                        })
                                        .collect(MoreCollectors.toImmutableList())
                        );
                    });
        });
    }

    private ChannelGroup<ChannelNumbering> addChannelNumbersToChannelGroup(
            NumberedChannelGroup channelGroup,
            Map<Id, ChannelGroup<?>> resolvedChannelGroups,
            LocalDate now
    ) {
        if (!channelGroup.getChannelNumbersFrom().isPresent()) {
            return channelGroup;
        }
        ChannelGroup<?> baseChannelGroup = resolvedChannelGroups.get(
                channelGroup.getChannelNumbersFrom().get().getId()
        );
        if (!(baseChannelGroup instanceof NumberedChannelGroup)) {
            return channelGroup;
        }
        NumberedChannelGroup baseNumberedChannelGroup = (NumberedChannelGroup) baseChannelGroup;
        ImmutableSet.Builder<ChannelNumbering> newChannelNumberings = ImmutableSet.builder();
        ImmutableMap.Builder<Id, ChannelNumbering> baseChannelNumberingsByIdBuilder = ImmutableMap.builder();
        Set<Id> baseChannelIdsAdded = new HashSet<>();
        for (ChannelNumbering channelNumbering : baseNumberedChannelGroup.getChannels()) {
            Id channelId = channelNumbering.getChannel().getId();
            if (channelNumbering.isAvailable(now) && !baseChannelIdsAdded.contains(channelId)) {
                baseChannelNumberingsByIdBuilder.put(channelId, channelNumbering);
                baseChannelIdsAdded.add(channelId);
            }
        }
        ImmutableMap<Id, ChannelNumbering> baseChannelNumberingsById = baseChannelNumberingsByIdBuilder.build();
        Set<Id> includedChannelIds = new HashSet<>();
        for (ChannelNumbering channelNumbering :
                channelGroup.getChannels(NumberedChannelGroup.ChannelOrdering.SPECIFIED)
        ) {
            if (includedChannelIds.contains(channelNumbering.getChannel().getId())) {
                // Since we're only adding the channel numbers which are currently available this prevents outputting
                // the same channel with the same number.
                continue;
            }
            ChannelNumbering baseChannelNumbering = baseChannelNumberingsById.get(
                    channelNumbering.getChannel().getId()
            );

            ChannelGroupMembership.Builder newChannelNumbering = ChannelGroupMembership.builder(
                    channelNumbering.getChannelGroup().getSource()
            )
                    .withChannelGroupId(channelNumbering.getChannelGroup().getId().longValue())
                    .withChannelId(channelNumbering.getChannel().getId().longValue());

            if (baseChannelNumbering != null) {
                if (baseChannelNumbering.getChannelNumber().isPresent()) {
                    newChannelNumbering.withChannelNumber(baseChannelNumbering.getChannelNumber().get());
                }
                if (baseChannelNumbering.getStartDate().isPresent()) {
                    newChannelNumbering.withStartDate(baseChannelNumbering.getStartDate().get());
                }
                if (baseChannelNumbering.getEndDate().isPresent()) {
                    newChannelNumbering.withEndDate(baseChannelNumbering.getEndDate().get());
                }
            }
            newChannelNumberings.add(newChannelNumbering.buildChannelNumbering());
            includedChannelIds.add(channelNumbering.getChannel().getId());
        }
        return channelGroup.copyWithChannels(newChannelNumberings.build());
    }


}
