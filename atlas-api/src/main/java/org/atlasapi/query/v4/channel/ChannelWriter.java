package org.atlasapi.query.v4.channel;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;
import org.atlasapi.output.writers.ImageListWriter;
import org.atlasapi.output.writers.RelatedLinkWriter;
import org.atlasapi.query.common.MissingResolvedDataException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;

import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;
import static org.atlasapi.output.writers.SourceWriter.sourceWriter;
import static org.elasticsearch.common.base.Preconditions.checkNotNull;

public class ChannelWriter implements EntityListWriter<ResolvedChannel> {

    private static final EntityListWriter<Publisher> AVAILABLE_FROM_WRITER = sourceListWriter(
            "available_from");
    private static final EntityWriter<Publisher> BROADCASTER_WRITER = sourceWriter("broadcaster");
    private static final AliasWriter ALIAS_WRITER = new AliasWriter();
    private static final ImageListWriter IMAGE_WRITER = new ImageListWriter();
    private static final RelatedLinkWriter RELATED_LINKS_WRITER = new RelatedLinkWriter();

    private final String listName;
    private final String fieldName;
    private final NumberToShortStringCodec idCode = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelGroupSummaryWriter channelGroupSummaryWriter;

    public ChannelWriter(
            String listName,
            String fieldName,
            ChannelGroupSummaryWriter channelGroupSummaryWriter
    ) {
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
    public void write(@Nonnull ResolvedChannel entity, @Nonnull FieldWriter format,
            @Nonnull OutputContext ctxt) throws IOException {
        format.writeField("title", entity.getChannel().getTitle());
        format.writeField("id", idCode.encode(entity.getChannel().getId().toBigInteger()));
        format.writeField("uri", entity.getChannel().getCanonicalUri());
        format.writeList(IMAGE_WRITER, entity.getChannel().getImages(), ctxt);
        format.writeList(AVAILABLE_FROM_WRITER, entity.getChannel().getAvailableFrom(), ctxt);
        format.writeObject(AVAILABLE_FROM_WRITER, entity.getChannel().getSource(), ctxt);
        format.writeField("media_type", entity.getChannel().getMediaType());
        format.writeObject(BROADCASTER_WRITER, entity.getChannel().getBroadcaster(), ctxt);
        format.writeList(ALIAS_WRITER, entity.getChannel().getAliases(), ctxt);
        format.writeList("genres", "genres", entity.getChannel().getGenres(), ctxt);
        format.writeField("high_definition", entity.getChannel().getHighDefinition());
        format.writeField("regional", entity.getChannel().getRegional());
        format.writeList(RELATED_LINKS_WRITER, entity.getChannel().getRelatedLinks(), ctxt);
        format.writeField("start_date", entity.getChannel().getStartDate());
        format.writeField("advertised_from", entity.getChannel().getAdvertiseFrom());

        if (hasChannelGroupSummaryAnnotation(ctxt)) {

            Optional<List<ChannelGroupSummary>> channelGroupSummaries =
                    entity.getChannelGroupSummaries();

            if(channelGroupSummaries.isPresent()) {
                format.writeList(channelGroupSummaryWriter, channelGroupSummaries.get(), ctxt);
            } else {
                throw new MissingResolvedDataException(channelGroupSummaryWriter.listName());
            }

        }

    }

    @Nonnull
    @Override
    public String fieldName(ResolvedChannel entity) {
        return fieldName;
    }

    private boolean hasChannelGroupSummaryAnnotation(OutputContext ctxt) {
        return !Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
                &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        ).contains(Annotation.CHANNEL_GROUPS_SUMMARY.toKey());
    }
}
