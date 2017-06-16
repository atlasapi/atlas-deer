package org.atlasapi.query.v4.channel;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.StreamSupport;

import javax.servlet.http.HttpServletRequest;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.output.ChannelMerger;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelQueryResultWriter extends QueryResultWriter<ResolvedChannel> {

    private final EntityListWriter<ResolvedChannel> channelListWriter;
    private final ChannelMerger merger;

    public ChannelQueryResultWriter(
            EntityListWriter<ResolvedChannel> channelListWriter,
            EntityWriter<Object> licenseWriter,
            EntityWriter<HttpServletRequest> requestWriter,
            ChannelMerger merger
    ) {
        super(licenseWriter, requestWriter);
        this.channelListWriter = checkNotNull(channelListWriter);
        this.merger = checkNotNull(merger);
    }

    @Override
    protected void writeResult(QueryResult<ResolvedChannel> result, ResponseWriter writer)
            throws IOException {

        OutputContext ctxt = OutputContext.valueOf(result.getContext());

        if (result.isListResult()) {
            FluentIterable<ResolvedChannel> resources = result.getResources();

            writer.writeList(channelListWriter, mergeEquivalents(resources, ctxt), ctxt);
        } else {
            ResolvedChannel resolvedChannel = result.getOnlyResource();

            writer.writeObject(channelListWriter, mergeEquivalents(resolvedChannel, ctxt), ctxt);
        }

    }

    private ResolvedChannel mergeEquivalents(ResolvedChannel resource, OutputContext ctxt) {
        return Iterables.getOnlyElement(
                mergeEquivalents(Collections.singleton(resource), ctxt)
        );
    }

    private Iterable<ResolvedChannel> mergeEquivalents(
            Iterable<ResolvedChannel> resources,
            OutputContext ctxt
    ) {
        ImmutableSet.Builder<ResolvedChannel> mergedSet = ImmutableSet.builder();

        StreamSupport.stream(resources.spliterator(), false)
                .forEach(resolvedChannel -> {
                    if (resolvedChannel.getEquivalents().isPresent()) {
                        mergedSet.add(
                                ResolvedChannel.Builder.copyOf(resolvedChannel).withChannel(
                                        merger.merge(
                                                ctxt,
                                                resolvedChannel.getChannel(),
                                                resolvedChannel.getEquivalents().get()
                                        )
                                ).build()
                        );
                    } else {
                        mergedSet.add(resolvedChannel);
                    }
                });

        return mergedSet.build();
    }
}
