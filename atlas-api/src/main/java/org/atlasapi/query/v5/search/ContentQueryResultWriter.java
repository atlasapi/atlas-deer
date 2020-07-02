package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Content;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.v5.search.attribute.SherlockParameter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.promise.Promise;

import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
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
                .getParameter(SherlockParameter.SCHEDULE_CHANNEL_GROUP.getParameterName());

        OutputContext.Builder builder = OutputContext.builder(queryContext);

        if (channelGroupParam != null) {
            List<String> channelGroupsParams = SPLITTER.splitToList(channelGroupParam);
            List<ChannelGroup<?>> channelGroups = resolveChannelGroups(channelGroupsParams);

            List<Region> regions = channelGroups.stream()
                    .filter(cg -> cg instanceof Region)
                    .map(cg -> (Region) cg)
                    .collect(Collectors.toList());

            if (!regions.isEmpty()) {
                builder.withRegions(regions);
            }

            List<Platform> platforms = channelGroups.stream()
                    .filter(cg -> cg instanceof Platform)
                    .map(cg -> (Platform) cg)
                    .collect(Collectors.toList());

            if (!platforms.isEmpty()) {
                builder.withPlatforms(platforms);
            }
        }

        return builder.build();
    }

    private List<ChannelGroup<?>> resolveChannelGroups(List<String> idParams) {

        List<Id> ids = idParams.stream()
                .map(codec::decode)
                .map(Id::valueOf)
                .collect(Collectors.toList());

        return Promise.wrap(channelGroupResolver.resolveIds(ids))
                .then(Resolved::getResources)
                .get(1, TimeUnit.MINUTES)
                .toList();
    }
}