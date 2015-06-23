package org.atlasapi.output.annotation;

import org.atlasapi.content.EpisodeSummary;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SubItemSummaryListWriter implements EntityListWriter<ItemSummary> {

    private final ItemRefWriter itemRefWriter;

    public SubItemSummaryListWriter(ItemRefWriter itemRefWriter) {
        this.itemRefWriter = checkNotNull(itemRefWriter);
    }



    @Nonnull
    @Override
    public String listName() {
        return "sub_item_summaries";
    }

    @Override
    public void write(@Nonnull ItemSummary entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        writer.writeObject(itemRefWriter, entity.getItemRef(), ctxt);
        writer.writeField("title", entity.getTitle());
        writer.writeField("description", entity.getDescription().orElse(null));
        writer.writeField("image", entity.getImage().orElse(null));
        if (entity instanceof EpisodeSummary) {
            writer.writeField("episode_number", ((EpisodeSummary)entity).getEpisodeNumber().orElse(null));
        }

    }

    @Nonnull
    @Override
    public String fieldName(ItemSummary entity) {
        return "sub_item_summary";
    }
}
