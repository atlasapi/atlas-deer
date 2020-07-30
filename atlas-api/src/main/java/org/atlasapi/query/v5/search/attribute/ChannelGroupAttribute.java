package org.atlasapi.query.v5.search.attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.query.common.coercers.IdCoercer;

import com.metabroadcast.promise.Promise;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

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
        for (ChannelGroup<?> channelGroup : resolveChannelGroups(values)) {
            if (channelGroup instanceof Region || channelGroup instanceof Platform) {
                Iterable<ChannelNumbering> channels;
                if (channelGroup instanceof Region) {
                    channels = ((Region) channelGroup).getChannels();
                } else {
                    channels = ((Platform) channelGroup).getChannels();
                }
                for (ChannelNumbering channelNumbering : channels) {
                    Id channelId = channelNumbering.getChannel().getId();
                    channelFilters.add(TermParameter.of(mapping, channelId.longValue()));
                }
            }
        }
        return channelFilters;
    }

    private List<ChannelGroup<?>> resolveChannelGroups(@Nonnull List<Id> channelGroupIds) {
        return Promise.wrap(channelGroupResolver.resolveIds(channelGroupIds))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES)
                .toList();
    }
}
