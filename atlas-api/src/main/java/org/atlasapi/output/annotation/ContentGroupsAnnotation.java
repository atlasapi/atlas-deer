package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.entity.Id;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ResolvedContent;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ContentGroupsAnnotation extends OutputAnnotation<Content> {

    public static final class ContentGroupWriter implements EntityListWriter<ContentGroup> {

        private final ItemRefWriter childRefWriter;

        public ContentGroupWriter(NumberToShortStringCodec idCodec) {
            childRefWriter = new ItemRefWriter(idCodec, "content");
        }

        @Override
        public void write(ContentGroup entity, FieldWriter writer, OutputContext ctxt)
                throws IOException {
            //TODO: introduce contentref writer. writer.writeList(childRefWriter, entity.getContents(), ctxt);
        }

        @Override
        public String listName() {
            return "content_groups";
        }

        @Override
        public String fieldName(ContentGroup entity) {
            return "content_group";
        }
    }

    private final ContentGroupResolver contentGroupResolver;

    public ContentGroupsAnnotation(NumberToShortStringCodec idCodec,
            ContentGroupResolver resolver) {
        this.contentGroupResolver = resolver;
        contentGroupWriter = new ContentGroupWriter(idCodec);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(contentGroupWriter, resolveRefs(entity.getContentGroupRefs()), ctxt);
    }

    private Iterable<ContentGroup> resolveRefs(List<ContentGroupRef> refs) {
        return resolveContentGroups(Lists.transform(refs, REF_TO_ID));
    }

    private Iterable<ContentGroup> resolveContentGroups(List<Id> contentGroups) {
        if (contentGroups.isEmpty()) {
            return ImmutableList.of();
        }
        ResolvedContent resolved = contentGroupResolver.findByIds(Lists.transform(
                contentGroups,
                Id.toLongValue()
        ));
        return Iterables.filter(resolved.asResolvedMap().values(), ContentGroup.class);
    }

    private static final Function<ContentGroupRef, Id> REF_TO_ID = new Function<ContentGroupRef, Id>() {

        @Override
        public Id apply(ContentGroupRef input) {
            return input.getId();
        }
    };
    private final ContentGroupWriter contentGroupWriter;
}
