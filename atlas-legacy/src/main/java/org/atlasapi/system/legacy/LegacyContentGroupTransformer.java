package org.atlasapi.system.legacy;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.content.ContentRef;
import org.atlasapi.entity.Alias;
import org.atlasapi.util.MorePredicates;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;

public class LegacyContentGroupTransformer extends
        DescribedLegacyResourceTransformer<org.atlasapi.media.entity.ContentGroup, ContentGroup> {

    @Override
    protected ContentGroup createDescribed(org.atlasapi.media.entity.ContentGroup legacyGroup) {

        ContentGroup contentGroup = new ContentGroup(
                legacyGroup.getCanonicalUri(),
                legacyGroup.getPublisher()
        );

        contentGroup.setId(legacyGroup.getId());
        transformInto(contentGroup, legacyGroup);

        return contentGroup;
    }

    @Override
    protected Iterable<Alias> moreAliases(org.atlasapi.media.entity.ContentGroup input) {
        return ImmutableList.of();
    }

    public static void transformInto(ContentGroup contentGroup,
            org.atlasapi.media.entity.ContentGroup input) {
        contentGroup.setType(ContentGroup.Type.valueOf(input.getType().name()));

        ImmutableList<ContentRef> refs = input.getContents().stream()
                .map(ref -> LegacyContentTransformer.legacyRefToRef(ref, input.getPublisher()))
                .filter(MorePredicates.isNotNull())
                .collect(MoreCollectors.toList());

        contentGroup.setContents(refs);
    }
}
