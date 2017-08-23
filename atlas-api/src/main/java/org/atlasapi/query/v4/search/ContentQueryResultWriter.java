package org.atlasapi.query.v4.search;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.Platform;
import org.atlasapi.channel.Region;
import org.atlasapi.content.Content;
import org.atlasapi.content.ResolvedContent;
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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentQueryResultWriter extends QueryResultWriter<ResolvedContent> {

    private final EntityListWriter<ResolvedContent> contentListWriter;
    private final NumberToShortStringCodec codec;

    public ContentQueryResultWriter(
            EntityListWriter<ResolvedContent> contentListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter,
            NumberToShortStringCodec codec
    ) {
        super(licenseWriter, requestWriter);
        this.contentListWriter = checkNotNull(contentListWriter);
        this.codec = checkNotNull(codec);
    }

    @Override
    protected void writeResult(QueryResult<ResolvedContent> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<ResolvedContent> resources = result.getResources();
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

        OutputContext.Builder builder = OutputContext.builder(queryContext);

//        if (!Strings.isNullOrEmpty(regionParam)) {
//            builder.withRegion(resolveRegion(regionParam));
//        }
//
//        if (!Strings.isNullOrEmpty(platformParam)) {
//            builder.withPlatform(resolvePlatform(platformParam));
//        }

        return builder.build();
    }

//    private Region resolveRegion(String id) throws IOException {
//        ChannelGroup<?> channelGroup = resolveChannelGroup(id);
//        if (channelGroup instanceof Region) {
//            return (Region) channelGroup;
//        } else {
//            throw new RuntimeException(new NotAcceptableException(
//                    String.format(
//                            "%s is a channel group of type '%s', should be 'region",
//                            id,
//                            channelGroup
//                    )
//            ));
//        }
//    }
//
//    private Platform resolvePlatform(String id) throws IOException {
//
//        ChannelGroup<?> channelGroup = resolveChannelGroup(id);
//        if (channelGroup instanceof Platform) {
//            return (Platform) channelGroup;
//        } else {
//            throw new RuntimeException(new NotAcceptableException(
//                    String.format(
//                            "%s is a channel group of type '%s', should be 'platform",
//                            id,
//                            channelGroup
//                    )
//            ));
//        }
//    }

//    @Nullable //TODO: resolveContent channel groups
//    private ChannelGroup<?> resolveChannelGroup(String idParam) throws IOException {
//
//        Id id = Id.valueOf(codec.decode(idParam));
//
//        Optional<ChannelGroup<?>> channelGroupOptional =  Futures.getChecked(
//                channelGroupResolver.resolveIds(ImmutableList.of(id)),
//                IOException.class,
//                1,
//                TimeUnit.MINUTES
//        ).getResources().first();
//
//        if (channelGroupOptional.isPresent()) {
//            return channelGroupOptional.get();
//        } else {
//           throw new RuntimeException(new NotFoundException(id));
//        }
//    }
}
