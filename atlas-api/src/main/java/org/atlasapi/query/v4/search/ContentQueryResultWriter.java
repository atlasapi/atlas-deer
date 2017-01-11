package org.atlasapi.query.v4.search;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
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
import com.google.common.base.Strings;
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
        if (Strings.isNullOrEmpty(regionParam)) {
            return OutputContext.valueOf(queryContext);
        }

        Id regionId = Id.valueOf(codec.decode(regionParam));
        com.google.common.base.Optional<ChannelGroup<?>> channelGroup = Futures.get(
                channelGroupResolver.resolveIds(ImmutableList.of(regionId)),
                1, TimeUnit.MINUTES,
                IOException.class
        ).getResources().first();
        if (!channelGroup.isPresent()) {
            Throwables.propagate(new NotFoundException(regionId));
        } else if (!(channelGroup.get() instanceof Region)) {
            Throwables.propagate(new NotAcceptableException(
                            String.format(
                                    "%s is a channel group of type '%s', should be 'region",
                                    regionParam,
                                    channelGroup.get().getType()
                            )
                    )
            );
        }

        return OutputContext.valueOf(queryContext, (Region) channelGroup.get());
    }
}
