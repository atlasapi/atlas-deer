package org.atlasapi.query.v4.schedule;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.NumberedChannelGroup;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
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
    public ListenableFuture<Resolved<ChannelGroup<?>>> allChannels() {
        return handleResult(delegate.allChannels(), false);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids) {
        return handleResult(delegate.resolveIds(ids), false);
    }

    @Override
    public ListenableFuture<Resolved<ChannelGroup<?>>> resolveIds(Iterable<Id> ids, Boolean refreshCache) {
        return handleResult(delegate.resolveIds(ids, refreshCache), refreshCache);
    }

    private ListenableFuture<Resolved<ChannelGroup<?>>> handleResult(
            ListenableFuture<Resolved<ChannelGroup<?>>> delegateResult,
            Boolean refreshCache
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
            ListenableFuture<Resolved<ChannelGroup<?>>> additionalChannelGroupsFuture
                    = delegate.resolveIds(additionalChannelGroupsToResolve, refreshCache);

            return Futures.transform(additionalChannelGroupsFuture,
                    (Function<Resolved<ChannelGroup<?>>, Resolved<ChannelGroup<?>>>) additionalChannelGroups -> {
                        ImmutableMap.Builder<Id, ChannelGroup<?>> additionalChannelGroupByIdMapBuilder
                                = ImmutableMap.builder();
                        additionalChannelGroupByIdMapBuilder.putAll(channelGroupById);
                        for (ChannelGroup<?> channelGroup : additionalChannelGroups.getResources()) {
                            additionalChannelGroupByIdMapBuilder.put(channelGroup.getId(), channelGroup);
                        }
                        ImmutableMap<Id, ChannelGroup<?>> additionalChannelGroupByIdMap
                                = additionalChannelGroupByIdMapBuilder.build();

                        LocalDate now = LocalDate.now();
                        for (NumberedChannelGroup channelGroupToFillChannelNumbers : channelGroupsToFillChannelNumbers) {
                            addChannelNumbersToChannelGroup(
                                    channelGroupToFillChannelNumbers,
                                    additionalChannelGroupByIdMap,
                                    now
                            );
                        }
                        return Resolved.valueOf(channelGroups);

                    });
        });
    }

    private void addChannelNumbersToChannelGroup(
            NumberedChannelGroup channelGroup,
            Map<Id, ChannelGroup<?>> resolvedChannelGroups,
            LocalDate now
    ) {
        if (!channelGroup.getChannelNumbersFrom().isPresent()) {
            return;
        }
        ChannelGroup<?> baseChannelGroup = resolvedChannelGroups.get(
                channelGroup.getChannelNumbersFrom().get().getId()
        );
        if (!(baseChannelGroup instanceof NumberedChannelGroup)) {
            return;
        }
        NumberedChannelGroup baseNumberedChannelGroup = (NumberedChannelGroup) baseChannelGroup;
        ImmutableSet.Builder<ChannelNumbering> newChannelNumberings = ImmutableSet.builder();
        ImmutableMap<Id, ChannelNumbering> baseChannelNumberingsById = MoreStreams.stream(
                baseNumberedChannelGroup.getChannels()
        )
                .filter(channelGroupMembership -> channelGroupMembership.isAvailable(now))
                .collect(
                        MoreCollectors.toImmutableMap(
                                channelGroupMembership -> channelGroupMembership.getChannel().getId(),
                                channelGroupMembership -> channelGroupMembership
                        )
                );
        Set<Id> includedChannelIds = new HashSet<>();
        for (ChannelNumbering channelNumbering : channelGroup.getChannels()) {
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
        channelGroup.setChannels(newChannelNumberings.build());
    }


}
