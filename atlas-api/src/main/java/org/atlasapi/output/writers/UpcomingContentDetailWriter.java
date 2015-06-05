package org.atlasapi.output.writers;

import com.google.common.collect.Iterables;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class UpcomingContentDetailWriter implements EntityListWriter<Item> {

    private final BroadcastWriter broadcastWriter;
    private final ItemDetailWriter itemDetailWriter;

    public UpcomingContentDetailWriter(BroadcastWriter broadcastWriter, ItemDetailWriter itemDetailWriter) {
        this.broadcastWriter = checkNotNull(broadcastWriter);
        this.itemDetailWriter = checkNotNull(itemDetailWriter);
    }

    @Override
    public String listName() {
        return "upcoming_content_detail";
    }

    @Override
    public void write(@Nonnull Item entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        List<Broadcast> sortedBroadcasts = entity.getBroadcasts()
                .stream()
                .sorted(Broadcast.startTimeOrdering())
                .collect(Collectors.toList());

        broadcastWriter.write(Iterables.getFirst(sortedBroadcasts, null), writer, ctxt);
        itemDetailWriter.write(entity, writer, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(Item entity) {
        return null;
    }
}
