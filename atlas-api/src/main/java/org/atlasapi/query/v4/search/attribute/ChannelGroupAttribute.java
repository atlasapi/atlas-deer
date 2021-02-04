package org.atlasapi.query.v4.search.attribute;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.promise.Promise;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupRef;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.query.common.coercers.IdCoercer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ChannelGroupAttribute extends
        SherlockSingleMappingAttributeList<Id, Long, KeywordMapping<Long>> {

    private final ChannelGroupResolver channelGroupResolver;

    public ChannelGroupAttribute(
            SherlockParameter parameter,
            KeywordMapping<Long> mapping,
            IdCoercer coercer,
            ChannelGroupResolver channelGroupResolver
    ) {
        super(parameter, coercer, mapping);
        this.channelGroupResolver = channelGroupResolver;
    }

    @Override
    protected List<SingleValueParameter<Long>> createParameters(KeywordMapping<Long> mapping, @Nonnull List<Id> values) {
        List<SingleValueParameter<Long>> channelFilters = new ArrayList<>();
        Set<Id> valueSet = ImmutableSet.copyOf(values);
        ChannelAndRegionIds channelAndRegionIds = getChannelAndRegionIds(resolveChannelGroups(valueSet));
        Set<Id> unresolvedRegions = Sets.difference(channelAndRegionIds.getRegionIds(), valueSet);
        ChannelAndRegionIds channelIdsFromRegions = getChannelAndRegionIds(resolveChannelGroups(unresolvedRegions));

        Set<Id> channelIds = Sets.union(channelAndRegionIds.getChannelIds(), channelIdsFromRegions.getChannelIds());
        // N.B. If this set of channelIds is empty then no filtering will be happening; it might make more sense
        // to return no content in such a scenario. For now this is left as-is since it's unlikely to be a common
        // problem.
        for (Id channelId : channelIds) {
            channelFilters.add(TermParameter.of(mapping, channelId.longValue()));
        }
        return channelFilters;
    }

    private ChannelAndRegionIds getChannelAndRegionIds(Iterable<ChannelGroup<?>> channelGroups) {
        ImmutableSet.Builder<Id> channelIds = ImmutableSet.builder();
        ImmutableSet.Builder<Id> regionIds = ImmutableSet.builder();
        for (ChannelGroup<?> channelGroup : channelGroups) {
            if (channelGroup instanceof Region || channelGroup instanceof Platform) {
                Iterable<ChannelNumbering> channels;
                if (channelGroup instanceof Region) {
                    channels = ((Region) channelGroup).getChannels();
                } else {
                    Platform platform = (Platform) channelGroup;
                    channels = platform.getChannels();
                    for (ChannelGroupRef region : platform.getRegions()) {
                        regionIds.add(region.getId());
                    }
                }
                for (ChannelNumbering channelNumbering : channels) {
                    channelIds.add(channelNumbering.getChannel().getId());
                }
            }
        }

        return new ChannelAndRegionIds(channelIds.build(), regionIds.build());
    }

    private List<ChannelGroup<?>> resolveChannelGroups(@Nonnull Iterable<Id> channelGroupIds) {
        return Promise.wrap(channelGroupResolver.resolveIds(channelGroupIds))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES)
                .toList();
    }

    private static class ChannelAndRegionIds {
        private final Set<Id> channelIds;
        private final Set<Id> regionIds;

        public ChannelAndRegionIds(Set<Id> channelIds, Set<Id> regionIds) {
            this.channelIds = channelIds;
            this.regionIds = regionIds;
        }

        public Set<Id> getChannelIds() {
            return channelIds;
        }

        public Set<Id> getRegionIds() {
            return regionIds;
        }
    }
}
