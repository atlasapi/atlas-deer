package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemDetailWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

public class AvailableContentDetailAnnotation extends OutputAnnotation<Content, ResolvedContent> { //TODO: add resolution

    public static final String AVAILABLE_CONTENT_DETAIL = "available_content_detail";

    private final MergingEquivalentsResolver<Content> contentResolver;
    private final ItemDetailWriter itemDetailWriter;

    public AvailableContentDetailAnnotation(
            MergingEquivalentsResolver<Content> contentResolver,
            ItemDetailWriter itemDetailWriter
    ) {
        this.contentResolver = contentResolver;
        this.itemDetailWriter = itemDetailWriter;
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (!(entity.getContent() instanceof Container)) {
            return;
        }

        Container container = (Container) entity.getContent();
        if (container.getAvailableContent().isEmpty()) {
            writer.writeList(itemDetailWriter, ImmutableList.of(), ctxt);
            return;
        }

        Set<Id> contentIds = container.getAvailableContent().keySet()
                .stream()
                .map(ResourceRef::getId)
                .collect(Collectors.toSet());

        final ResolvedEquivalents<Content> resolvedEquivalents = Futures.get(
                contentResolver.resolveIds(
                        contentIds,
                        ctxt.getApplication(),
                        Annotation.all()
                ),
                IOException.class
        );

        Iterable<ResolvedContent> items = StreamSupport.stream(resolvedEquivalents.getFirstElems()
                .spliterator(), false)
                .map(c -> (Item) c)
                .map(ResolvedContent::wrap)
                .collect(Collectors.toList());

        writer.writeList(itemDetailWriter, items, ctxt);
    }
}
