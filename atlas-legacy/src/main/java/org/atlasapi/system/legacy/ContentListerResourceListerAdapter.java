package org.atlasapi.system.legacy;

import java.util.Iterator;

import com.google.common.base.Function;
import org.atlasapi.content.Content;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.source.Sources;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentListerResourceListerAdapter implements ResourceLister<Content> {

    private static final Logger log = LoggerFactory.getLogger(ContentListerResourceListerAdapter.class);
    private final ContentLister contentLister;
    private final LegacyContentTransformer transformer;

    public ContentListerResourceListerAdapter(
            ContentLister contentLister,
            LegacyContentTransformer transformer
    ) {
        this.contentLister = checkNotNull(contentLister);
        this.transformer = checkNotNull(transformer);
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
                        new Function<org.atlasapi.media.entity.Content, Content>() {
                            @Override
                            public Content apply(org.atlasapi.media.entity.Content input) {
                                try {
                                    return transformer.apply(input);
                                } catch (Exception e) {
                                    log.warn(
                                            "Exception while bootstrapping content with ID {}, '{}'",
                                            input.getId(),
                                            e.getMessage(),
                                            e
                                    );
                                    return null;
                                }
                            }
                        }
                );
            }
        };
    }

}
