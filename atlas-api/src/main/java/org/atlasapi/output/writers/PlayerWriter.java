package org.atlasapi.output.writers;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.content.Player;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;

public class PlayerWriter implements EntityWriter<Player> {

    private final ImageListWriter imageListWriter = new ImageListWriter();
    private final RelatedLinkWriter relatedLinkWriter = new RelatedLinkWriter();
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    @Override
    public void write(@Nonnull Player entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        writer.writeField("title", entity.getTitle());
        writer.writeField("title", entity.getTitle());
        writer.writeField("description", entity.getDescription());
        writer.writeField("image", entity.getImage());
        writer.writeField("thumbnail", entity.getThumbnail());
        writer.writeList(imageListWriter, entity.getImages(), ctxt);
        writer.writeList(relatedLinkWriter, entity.getRelatedLinks(), ctxt);
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
    }

    @Nonnull
    @Override
    public String fieldName(Player entity) {
        return "player";
    }
}
