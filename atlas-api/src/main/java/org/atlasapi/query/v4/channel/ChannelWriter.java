package org.atlasapi.query.v4.channel;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.channel.ChannelGroupSummary;
import org.atlasapi.channel.ResolvedChannel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.ChannelGroupSummaryWriter;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;
import org.atlasapi.output.writers.ChannelVariantRefWriter;
import org.atlasapi.output.writers.ImageListWriter;
import org.atlasapi.output.writers.RelatedLinkWriter;
import org.atlasapi.query.common.exceptions.MissingResolvedDataException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

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
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ChannelGroupSummaryWriter channelGroupSummaryWriter;
    private final ChannelVariantRefWriter includedVariantWriter;
    private final ChannelVariantRefWriter excludedVariantWriter;


    private ChannelWriter(
            String listName,
            String fieldName,
            ChannelGroupSummaryWriter channelGroupSummaryWriter
    ) {
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
        this.channelGroupSummaryWriter = checkNotNull(channelGroupSummaryWriter);

        includedVariantWriter = ChannelVariantRefWriter.create(
                "included_variant",
                "included_variants",
                idCodec
        );
        excludedVariantWriter = ChannelVariantRefWriter.create(
                "excluded_variant",
                "excluded_variants",
                idCodec
        );
    }

    public static ChannelWriter create(
            String listName,
            String fieldName,
            ChannelGroupSummaryWriter channelGroupSummaryWriter
    ) {
        return new ChannelWriter(listName, fieldName, channelGroupSummaryWriter);
    }

    @Nonnull
    @Override
    public String listName() {
        return listName;
    }

    @Override
    public void write(
            @Nonnull ResolvedChannel entity,
            @Nonnull FieldWriter format,
            @Nonnull OutputContext ctxt
    ) throws IOException {

        Channel channel = entity.getChannel();

        format.writeField("title", channel.getTitle());

        if (entity.getIncludedVariants().isPresent() || entity.getExcludedVariants().isPresent()) {
            format.writeList(
                    includedVariantWriter,
                    entity.getIncludedVariants().orElse(ImmutableList.of()),
                    ctxt
            );
            format.writeList(
                    excludedVariantWriter,
                    entity.getExcludedVariants().orElse(ImmutableList.of()),
                    ctxt
            );
        }

        format.writeField("id", idCodec.encode(channel.getId().toBigInteger()));
        format.writeField("uri", channel.getCanonicalUri());
        format.writeList(IMAGE_WRITER, channel.getImages(), ctxt);
        format.writeList(AVAILABLE_FROM_WRITER, channel.getAvailableFrom(), ctxt);
        format.writeObject(AVAILABLE_FROM_WRITER, channel.getSource(), ctxt);
        format.writeField("media_type", channel.getMediaType());
        format.writeObject(BROADCASTER_WRITER, channel.getBroadcaster(), ctxt);
        format.writeList(ALIAS_WRITER, channel.getAliases(), ctxt);
        format.writeList("genres", "genres", channel.getGenres(), ctxt);
        format.writeField("high_definition", channel.getHighDefinition());
        format.writeField("regional", channel.getRegional());
        format.writeList(RELATED_LINKS_WRITER, channel.getRelatedLinks(), ctxt);
        format.writeField("start_date", channel.getStartDate());
        format.writeField("advertised_from", channel.getAdvertiseFrom());
        format.writeField("short_description", channel.getShortDescription());
        format.writeField("medium_description", channel.getMediumDescription());
        format.writeField("long_description", channel.getLongDescription());
        format.writeField("region", channel.getRegion());
        format.writeList("target_regions", "target_region", channel.getTargetRegions(), ctxt);
        format.writeField("channel_type", channel.getChannelType());

        if (contextHasAnnotation(ctxt, Annotation.CHANNEL_GROUPS_SUMMARY) ||
                contextHasAnnotation(ctxt, Annotation.GENERIC_CHANNEL_GROUPS_SUMMARY)) {

            Optional<List<ChannelGroupSummary>> channelGroupSummaries =
                    entity.getChannelGroupSummaries();

            if(channelGroupSummaries.isPresent()) {
                format.writeList(channelGroupSummaryWriter, channelGroupSummaries.get(), ctxt);
            } else {
                throw new MissingResolvedDataException("channel writer group summaries");
            }
        }
    }

    @Nonnull
    @Override
    public String fieldName(ResolvedChannel entity) {
        return fieldName;
    }

    private boolean contextHasAnnotation(OutputContext ctxt, Annotation annotation) {

        return !com.google.common.base.Strings.isNullOrEmpty(ctxt.getRequest().getParameter("annotations"))
                &&
                Splitter.on(',')
                        .splitToList(
                                ctxt.getRequest().getParameter("annotations")
                        ).contains(annotation.toKey());
    }

}
