package org.atlasapi.query.v4.search;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import com.google.common.base.Optional;
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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentQueryResultWriter extends QueryResultWriter<Content> {

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
        String regionParam = queryContext.getRequest()
                .getParameter(Attributes.REGION.externalName());
        String platformParam = queryContext.getRequest()
                .getParameter(Attributes.PLATFORM.externalName());

        return OutputContext.valueOf(
                queryContext,
                resolveRegion(regionParam),
                resolvePlatform(platformParam)
        );
    }

    @Nullable
    private Region resolveRegion(String id) throws IOException {
        ChannelGroup<?> channelGroup = resolveChannelGroup(id);
        if (channelGroup instanceof Platform) {
            return (Region) channelGroup;
        } else {
            Throwables.propagate(new NotAcceptableException(
                    String.format(
                            "%s is a channel group of type '%s', should be 'region",
                            id,
                            channelGroup
                    )
            ));
            return null;
        }
    }

    @Nullable
    private Platform resolvePlatform(String id) throws IOException {

        ChannelGroup<?> channelGroup = resolveChannelGroup(id);
        if (channelGroup instanceof Platform) {
            return (Platform) channelGroup;
        } else {
            Throwables.propagate(new NotAcceptableException(
                    String.format(
                            "%s is a channel group of type '%s', should be 'platform",
                            id,
                            channelGroup
                    )
            ));
            return null;
        }
    }

    @Nullable
    private ChannelGroup<?> resolveChannelGroup(String idParam) throws IOException {

        Id id = Id.valueOf(codec.decode(idParam));
        
        Optional<ChannelGroup<?>> channelGroupOptional =  Futures.getChecked(
                channelGroupResolver.resolveIds(ImmutableList.of(id)),
                IOException.class,
                1,
                TimeUnit.MINUTES
        ).getResources().first();

        if (channelGroupOptional.isPresent()) {
            return channelGroupOptional.get();
        } else {
           Throwables.propagate(new NotFoundException(id));
        }
        return null;
    }
}
