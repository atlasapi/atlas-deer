package org.atlasapi.query.v5.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Content;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.NotAcceptableException;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentQueryResultWriter extends QueryResultWriter<Content> {

    private static final Logger log = LoggerFactory.getLogger(ContentQueryResultWriter.class);

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

    private OutputContext outputContext(QueryContext queryContext) throws IOException {

        String channelGroupParam = queryContext.getRequest()
                .getParameter(Attributes.CHANNEL_GROUP.externalName());

        OutputContext.Builder builder = OutputContext.builder(queryContext);

        List<String> channelGroups = Arrays.asList(channelGroupParam.split("\\s*,\\s*"));

        List<Region> regions = new ArrayList<>();
        List<Platform> platforms = new ArrayList<>();
        for (ChannelGroup<?> channelGroup : resolveChannelGroups(channelGroups)) {
            if (channelGroup instanceof Region) {
                regions.add((Region) channelGroup);
            } else if (channelGroup instanceof Platform) {
                platforms.add((Platform) channelGroup);
            } else {
                throw new RuntimeException(new NotAcceptableException(
                        String.format(
                                "%s is not a region or platform.",
                                channelGroup.getId()
                        )
                ));
            }
        }

        if (!regions.isEmpty()) {
            builder.withRegions(regions);
        }

        if (!platforms.isEmpty()) {
            builder.withPlatforms(platforms);
        }

        return builder.build();
    }

    private List<ChannelGroup<?>> resolveChannelGroups(List<String> idParams) {

        List<ChannelGroup<?>> channelGroups = new ArrayList<>();

        for (String idParam : idParams) {
            Id id = Id.valueOf(codec.decode(idParam));

            Optional<ChannelGroup<?>> channelGroupOptional;
            try {
                channelGroupOptional = Futures.getChecked(
                        channelGroupResolver.resolveIds(ImmutableList.of(id)),
                        IOException.class,
                        1,
                        TimeUnit.MINUTES
                ).getResources().first();
            } catch (IOException e) {
                throw new RuntimeException(new NotFoundException(id));
            }

            if (channelGroupOptional.isPresent()) {
                channelGroups.add(channelGroupOptional.get());
            } else {
                throw new RuntimeException(new NotFoundException(id));
            }
        }

        return channelGroups;
    }
}
