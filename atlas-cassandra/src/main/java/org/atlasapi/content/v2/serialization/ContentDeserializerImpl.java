package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.content.v2.serialization.setters.BrandSetter;
import org.atlasapi.content.v2.serialization.setters.ContainerSetter;
import org.atlasapi.content.v2.serialization.setters.ContentSetter;
import org.atlasapi.content.v2.serialization.setters.EpisodeSetter;
import org.atlasapi.content.v2.serialization.setters.FilmSetter;
import org.atlasapi.content.v2.serialization.setters.ItemSetter;
import org.atlasapi.content.v2.serialization.setters.SeriesSetter;
import org.atlasapi.content.v2.serialization.setters.SongSetter;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentDeserializerImpl implements ContentDeserializer {

    private final ContentSetter contentSetter = new ContentSetter();
    private final ContainerSetter containerSetter = new ContainerSetter();
    private final ItemSetter itemSetter = new ItemSetter();
    private final EpisodeSetter episodeSetter = new EpisodeSetter();
    private final SeriesSetter seriesSetter = new SeriesSetter();
    private final BrandSetter brandSetter = new BrandSetter();
    private final FilmSetter filmSetter = new FilmSetter();
    private final SongSetter songSetter = new SongSetter();

    @Override
    public Content deserialize(org.atlasapi.content.v2.model.Content row) {
        Content content = makeEmptyContent(checkNotNull(row));
        contentSetter.deserialize(content, row);

        switch (row.getType()) {
            case "item":
                itemSetter.deserialize(content, row);
                break;
            case "song":
                itemSetter.deserialize(content, row);
                songSetter.deserialize(content, row);
                break;
            case "episode":
                itemSetter.deserialize(content, row);
                episodeSetter.deserialize(content, row);
                break;
            case "film":
                itemSetter.deserialize(content, row);
                filmSetter.deserialize(content, row);
                break;
            case "brand":
                containerSetter.deserialize(content, row);
                brandSetter.deserialize(content, row);
                break;
            case "series":
                containerSetter.deserialize(content, row);
                seriesSetter.deserialize(content, row);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Illegal object type: %s", row.getType())
                );
        }

        return content;
    }

    private Content makeEmptyContent(org.atlasapi.content.v2.model.Content internal) {
        switch (internal.getType()) {
            case "item":
                return new Item();
            case "song":
                return new Song();
            case "episode":
                return new Episode();
            case "film":
                return new Film();
            case "brand":
                return new Brand();
            case "series":
                return new Series();
            default:
                throw new IllegalArgumentException(
                        String.format("Illegal object type: %s", internal.getType())
                );
        }
    }
}
