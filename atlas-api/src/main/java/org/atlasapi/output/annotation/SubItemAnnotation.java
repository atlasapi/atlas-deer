package org.atlasapi.output.annotation;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.ItemRef;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;

import java.io.IOException;


public class SubItemAnnotation extends OutputAnnotation<Content> {

    private final ItemRefWriter childRefWriter;

    public SubItemAnnotation(NumberToShortStringCodec idCodec) {
        childRefWriter = new ItemRefWriter(idCodec, "content");
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Container) {
            Container container = (Container) entity;
            ImmutableList<ItemRef> orderedRefs = Ordering.natural()
                    .onResultOf(ItemRef::getSortKey)
                    .nullsLast()
                    .immutableSortedCopy(container.getItemRefs());
            writer.writeList(childRefWriter, orderedRefs, ctxt);
        }
    }

}
