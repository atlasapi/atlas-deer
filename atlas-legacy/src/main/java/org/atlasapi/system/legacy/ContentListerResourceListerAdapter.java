package org.atlasapi.system.legacy;

import java.util.Iterator;

import org.atlasapi.content.Content;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.source.Sources;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;

public class ContentListerResourceListerAdapter implements ResourceLister<Content> {

    private final ContentLister contentLister;
    private final LegacyContentTransformer transformer;

    public ContentListerResourceListerAdapter(ContentLister contentLister, ChannelResolver channelResolver,
                                              LegacySegmentMigrator legacySegmentMigrator) {
        this.contentLister = contentLister;
        this.transformer = new LegacyContentTransformer(channelResolver, legacySegmentMigrator);
    }

    @Override
    public FluentIterable<Content> list() {
        return new FluentIterable<Content>() {
            @Override
            public Iterator<Content> iterator() {
                return Iterators.transform(
                        contentLister.listContent(
                                ContentListingCriteria.defaultCriteria()
                                        .forPublishers(Sources.all().asList())
                                        .forContent(
                                                ContentCategory.CONTAINER,
                                                ContentCategory.PROGRAMME_GROUP,
                                                ContentCategory.TOP_LEVEL_ITEM,
                                                ContentCategory.CHILD_ITEM
                                        )
                                        .build()
                        ),
                        transformer
                );
            }
        };
    }

}
