package org.atlasapi.system.legacy;

import java.util.Iterator;

import org.atlasapi.content.Content;
import org.atlasapi.entity.ResourceLister;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.source.Sources;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentListerResourceListerAdapter implements ResourceLister<Content> {

    private static final Logger log = LoggerFactory.getLogger(ContentListerResourceListerAdapter.class);
    private final LegacyContentLister contentLister;
    private final LegacyContentTransformer transformer;

    public ContentListerResourceListerAdapter(
            LegacyContentLister contentLister,
            LegacyContentTransformer transformer
    ) {
        this.contentLister = checkNotNull(contentLister);
        this.transformer = checkNotNull(transformer);
    }

    @Override
    public FluentIterable<Content> list(Iterable<Publisher> sources) {
        return list(sources, ContentListingProgress.START);
    }

    @Override
    public FluentIterable<Content> list() {
        return list(Sources.all().asList());
    }

    @Override
    public FluentIterable<Content> list(ContentListingProgress progress) {
        return list(Publisher.all(), progress);
    }

    @Override
    public FluentIterable<Content> list(Iterable<Publisher> sources,
            ContentListingProgress progress) {
        return new FluentIterable<Content>() {

            @Override
            public Iterator<Content> iterator() {
                return Iterators.transform(
                        contentLister.listContent(
                                ImmutableList.copyOf(sources),
                                progress
                        ),
                        input -> {
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
                );
            }
        };
    }
}
