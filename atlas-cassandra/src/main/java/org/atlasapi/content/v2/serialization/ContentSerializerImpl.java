package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.Content;

public class ContentSerializerImpl implements ContentSerializer {

    private final ContentSetter contentSetter = new ContentSetter();
    private final ContainerSetter containerSetter = new ContainerSetter();
    private final ItemSetter itemSetter = new ItemSetter();
    private final EpisodeSetter episodeSetter = new EpisodeSetter();
    private final SeriesSetter seriesSetter = new SeriesSetter();
    private final BrandSetter brandSetter = new BrandSetter();
    private final FilmSetter filmSetter = new FilmSetter();
    private final SongSetter songSetter = new SongSetter();

    @Override
    public Content serialize(org.atlasapi.content.Content content) {
        Content internal = new Content();
        setType(internal, content);

        contentSetter.serialize(internal, content);

        itemSetter.serialize(internal, content);
        songSetter.serialize(internal, content);
        episodeSetter.serialize(internal, content);
        filmSetter.serialize(internal, content);

        containerSetter.serialize(internal, content);
        brandSetter.serialize(internal, content);
        seriesSetter.serialize(internal, content);

        return internal;
    }

    private void setType(Content internal, org.atlasapi.content.Content content) {
        internal.setType(content.getClass().getSimpleName().toLowerCase());
    }
}
