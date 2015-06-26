package org.atlasapi.query.v4.channel;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;
import org.atlasapi.output.writers.ImageListWriter;
import org.atlasapi.output.writers.RelatedLinkWriter;
import org.atlasapi.util.ImmutableCollectors;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;
import static org.atlasapi.output.writers.SourceWriter.sourceWriter;
import static org.elasticsearch.common.base.Preconditions.checkNotNull;

public class ChannelWriter implements EntityListWriter<Channel>{

    private static final EntityListWriter<Publisher> AVAILABLE_FROM_WRITER = sourceListWriter("available_from");
    private static final EntityWriter<Publisher> BROADCASTER_WRITER = sourceWriter("broadcaster");
    private static final AliasWriter ALIAS_WRITER = new AliasWriter();
    private static final ImageListWriter IMAGE_WRITER = new ImageListWriter();
    private static final RelatedLinkWriter RELATED_LINKS_WRITER = new RelatedLinkWriter();
    private final ChannelGroupResolver channelGroupResolver;

    private final String listName;
    private final String fieldName;
    private final NumberToShortStringCodec idCode = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelGroupSummaryWriter channelGroupSummaryWriter;

    public ChannelWriter(
            ChannelGroupResolver channelGroupResolver,
            String listName,
            String fieldName,
            ChannelGroupSummaryWriter channelGroupSummaryWriter
    ) {
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
        this.channelGroupSummaryWriter = checkNotNull(channelGroupSummaryWriter);
    }



    @Nonnull
    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(@Nonnull Channel entity, @Nonnull FieldWriter format, @Nonnull OutputContext ctxt) throws IOException {
        format.writeField("title", entity.getTitle());
        format.writeField("id", idCode.encode(entity.getId().toBigInteger()));
        format.writeField("uri", entity.getCanonicalUri());
        format.writeList(IMAGE_WRITER, entity.getImages(), ctxt);
        format.writeList(AVAILABLE_FROM_WRITER, entity.getAvailableFrom(), ctxt);
        format.writeObject(AVAILABLE_FROM_WRITER, entity.getSource(), ctxt);
        format.writeField("media_type", entity.getMediaType());
        format.writeObject(BROADCASTER_WRITER, entity.getBroadcaster(), ctxt);
        format.writeList(ALIAS_WRITER, entity.getAliases(), ctxt);
        format.writeList("genres", "genres", entity.getGenres(), ctxt);
        format.writeField("high_definition", entity.getHighDefinition());
        format.writeField("regional", entity.getRegional());
        format.writeList(RELATED_LINKS_WRITER, entity.getRelatedLinks(), ctxt);
        format.writeField("start_date", entity.getStartDate());


        if(!Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations")) &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        ).contains(Annotation.CHANNEL_GROUPS_SUMMARY.toKey())
                ) {

            ImmutableList<Id> channelGroupIds = entity.getChannelGroups()
                    .stream()
                    .map(cg -> cg.getChannelGroup().getId())
                    .collect(ImmutableCollectors.toList());

            List<ChannelGroupSummary> channelGroupSummaries = StreamSupport.stream(
                    Futures.get(
                            channelGroupResolver.resolveIds(channelGroupIds),
                            1, TimeUnit.MINUTES,
                            IOException.class
                    ).getResources().spliterator(), false
            )
                    .map(ChannelGroup::toSummary)
                    .collect(ImmutableCollectors.toList());

            format.writeList(channelGroupSummaryWriter, channelGroupSummaries, ctxt);


        }

    }

    @Nonnull
    @Override
    public String fieldName(Channel entity) {
        return fieldName;
    }
}
