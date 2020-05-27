package org.atlasapi.query.v5.search.attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupMembership;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.promise.Promise;
import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class ChannelGroupAttribute extends SherlockAttributeList<Id, String, KeywordMapping> {

    private final NumberToShortStringCodec idCodec;
    private final ChannelGroupResolver channelGroupResolver;

    public ChannelGroupAttribute(
            SherlockParameter parameter,
            KeywordMapping mapping,
            NumberToShortStringCodec idCodec,
            ChannelGroupResolver channelGroupResolver
    ) {
        super(parameter, mapping, IdCoercer.create(idCodec));
        this.idCodec = idCodec;
        this.channelGroupResolver = channelGroupResolver;
    }

    @Override
    protected List<SimpleParameter<String>> createParameters(KeywordMapping mapping, @Nonnull List<Id> values) {
        List<SimpleParameter<String>> channelFilters = new ArrayList<>();
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
                    String encodedChannelId = idCodec.encode(channelId.toBigInteger());
                    channelFilters.add(TermParameter.of(mapping, encodedChannelId));
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
