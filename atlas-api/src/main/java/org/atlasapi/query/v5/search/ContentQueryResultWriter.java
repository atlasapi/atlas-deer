package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Content;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.v5.search.attribute.SherlockAttributes;

import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.promise.Promise;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentQueryResultWriter extends QueryResultWriter<Content> {

    private static final Logger log = LoggerFactory.getLogger(ContentQueryResultWriter.class);
    private final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

    private final EntityListWriter<Content> contentListWriter;
    private final ChannelGroupResolver channelGroupResolver;
    private final NumberToShortStringCodec codec;

    public ContentQueryResultWriter(
            EntityListWriter<Content> contentListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter,
            ChannelGroupResolver channelGroupResolver,
            NumberToShortStringCodec codec
    ) {
        super(licenseWriter, requestWriter);
        this.contentListWriter = checkNotNull(contentListWriter);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.codec = checkNotNull(codec);
    }

    @Override
    protected void writeResult(QueryResult<Content> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Content> resources = result.getResources();
            writer.writeList(contentListWriter, resources, ctxt);
        } else {
            writer.writeObject(contentListWriter, result.getOnlyResource(), ctxt);
        }

    }

    private OutputContext outputContext(QueryContext queryContext) {

        String channelGroupParam = queryContext.getRequest()
                .getParameter(SherlockAttributes.SCHEDULE_CHANNEL_GROUP_PARAM);

        OutputContext.Builder builder = OutputContext.builder(queryContext);

        List<String> channelGroupsParams = SPLITTER.splitToList(channelGroupParam);
        FluentIterable<ChannelGroup<?>> channelGroups = resolveChannelGroups(channelGroupsParams);

        List<Region> regions = channelGroups
                .filter(cg -> cg instanceof Region)
                .transform(cg -> (Region) cg)
                .toList();

        if (!regions.isEmpty()) {
            builder.withRegions(regions);
        }

        List<Platform> platforms = channelGroups
                .filter(cg -> cg instanceof Platform)
                .transform(cg -> (Platform) cg)
                .toList();

        if (!platforms.isEmpty()) {
            builder.withPlatforms(platforms);
        }

        return builder.build();
    }

    private FluentIterable<ChannelGroup<?>> resolveChannelGroups(List<String> idParams) {

        List<Id> ids = idParams.stream()
                .map(codec::decode)
                .map(Id::valueOf)
                .collect(Collectors.toList());

        return Promise.wrap(channelGroupResolver.resolveIds(ids))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES);
    }
}
