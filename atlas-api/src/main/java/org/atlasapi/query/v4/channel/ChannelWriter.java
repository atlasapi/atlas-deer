package org.atlasapi.query.v4.channel;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;
import org.atlasapi.output.writers.ImageListWriter;
import org.atlasapi.output.writers.RelatedLinkWriter;

import javax.annotation.Nonnull;
import java.io.IOException;

import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;
import static org.atlasapi.output.writers.SourceWriter.sourceWriter;
import static org.elasticsearch.common.base.Preconditions.checkNotNull;

public class ChannelWriter implements EntityListWriter<Channel>{

    private static final EntityListWriter<Publisher> AVAILABLE_FROM_WRITER = sourceListWriter("available_from");
    private static final EntityWriter<Publisher> BROADCASTER_WRITER = sourceWriter("broadcaster");
    private static final AliasWriter ALIAS_WRITER = new AliasWriter();
    private static final ImageListWriter IMAGE_WRITER = new ImageListWriter();
    private static final RelatedLinkWriter RELATED_LINKS_WRITER = new RelatedLinkWriter();

    private final String listName;
    private final String fieldName;
    private final NumberToShortStringCodec idCode = SubstitutionTableNumberCodec.lowerCaseOnly();

    public ChannelWriter(String listName, String fieldName) {
        this.listName = checkNotNull(listName);
        this.fieldName = checkNotNull(fieldName);
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
        format.writeObject(AVAILABLE_FROM_WRITER, entity.getPublisher(), ctxt);
        format.writeField("media_type", entity.getMediaType());
        format.writeObject(BROADCASTER_WRITER, entity.getBroadcaster(), ctxt);
        format.writeList(ALIAS_WRITER, entity.getAliases(), ctxt);
        format.writeList("genres", "genres", entity.getGenres(), ctxt);
        format.writeField("high_definition", entity.getHighDefinition());
        format.writeField("regional", entity.getRegional());
        format.writeField("adult", entity.getAdult());
        format.writeList(RELATED_LINKS_WRITER, entity.getRelatedLinks(), ctxt);
        format.writeField("start_date", entity.getStartDate());

    }

    @Nonnull
    @Override
    public String fieldName(Channel entity) {
        return fieldName;
    }
}
