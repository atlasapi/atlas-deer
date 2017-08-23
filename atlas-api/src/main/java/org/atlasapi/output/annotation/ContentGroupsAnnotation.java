package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.List;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.entity.Id;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ItemRefWriter;
import org.atlasapi.persistence.content.ContentGroupResolver;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ContentGroupsAnnotation extends OutputAnnotation<Content, ResolvedContent> { //TODO: add resolution

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
    private final ContentGroupWriter contentGroupWriter;

    public ContentGroupsAnnotation(
            NumberToShortStringCodec idCodec,
            ContentGroupResolver resolver
    ) {
        this.contentGroupResolver = resolver;
        this.contentGroupWriter = new ContentGroupWriter(idCodec);
    }

    @Override
    public void write(org.atlasapi.content.ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(contentGroupWriter, resolveRefs(entity.getContent().getContentGroupRefs()), ctxt);
    }

    private Iterable<ContentGroup> resolveRefs(List<ContentGroupRef> refs) {
        return resolveContentGroups(Lists.transform(refs, ContentGroupRef::getId));
    }

    private Iterable<ContentGroup> resolveContentGroups(List<Id> contentGroups) {
        if (contentGroups.isEmpty()) {
            return ImmutableList.of();
        }
        org.atlasapi.persistence.content.ResolvedContent resolved = contentGroupResolver.findByIds(Lists.transform(
                contentGroups,
                Id.toLongValue()
        ));
        return Iterables.filter(resolved.asResolvedMap().values(), ContentGroup.class);
    }
}
