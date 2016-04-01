package org.atlasapi.content.v2.serialization;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.content.v2.model.udt.Clip;
import org.atlasapi.content.v2.model.udt.Encoding;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentDeserializerImpl implements ContentDeserializer {

    private static final ObjectMapper mapper;
    private static final JavaType clipType;
    private static final JavaType encodingType;

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        clipType = mapper.getTypeFactory().constructCollectionType(List.class, Clip.class);
        encodingType = mapper.getTypeFactory().constructCollectionType(List.class, Encoding.class);
    }

    private final EncodingSerialization encoding = new EncodingSerialization();
    private final ContentSetter contentSetter = new ContentSetter();
    private final ClipSerialization clip = new ClipSerialization();
    private final ContainerSetter containerSetter = new ContainerSetter();
    private final ItemSetter itemSetter = new ItemSetter();
    private final EpisodeSetter episodeSetter = new EpisodeSetter();
    private final SeriesSetter seriesSetter = new SeriesSetter();
    private final BrandSetter brandSetter = new BrandSetter();
    private final FilmSetter filmSetter = new FilmSetter();
    private final SongSetter songSetter = new SongSetter();

    @Override
    public Content deserialize(Iterable<org.atlasapi.content.v2.model.Content> rows) {
        org.atlasapi.content.v2.model.Content main = null, clips = null, encodings = null;

        for (org.atlasapi.content.v2.model.Content row : rows) {
            switch (row.getDiscriminator()) {
                case org.atlasapi.content.v2.model.Content.ROW_MAIN:
                    main = row;
                    break;
                case org.atlasapi.content.v2.model.Content.ROW_CLIPS:
                    clips = row;
                    break;
                case org.atlasapi.content.v2.model.Content.ROW_ENCODINGS:
                    encodings = row;
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            "Illegal row discriminator: %s",
                            row.getDiscriminator()
                    ));
            }
        }

        Content content = makeEmptyContent(checkNotNull(main));
        contentSetter.deserialize(content, main);

        switch (main.getType()) {
            case "item":
                itemSetter.deserialize(content, main);
                break;
            case "song":
                itemSetter.deserialize(content, main);
                songSetter.deserialize(content, main);
                break;
            case "episode":
                itemSetter.deserialize(content, main);
                episodeSetter.deserialize(content, main);
                break;
            case "film":
                itemSetter.deserialize(content, main);
                filmSetter.deserialize(content, main);
                break;
            case "brand":
                containerSetter.deserialize(content, main);
                brandSetter.deserialize(content, main);
                break;
            case "series":
                containerSetter.deserialize(content, main);
                seriesSetter.deserialize(content, main);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Illegal object type: %s", main.getType())
                );
        }

        try {
            if (clips != null) {
                List<Clip> clipList = mapper.readValue(clips.getJsonBlob(), clipType);
                content.setClips(clipList.stream()
                        .map(clip::deserialize)
                        .collect(Collectors.toList()));
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        try {
            if (encodings != null) {
                List<Encoding> encodingList = mapper.readValue(encodings.getJsonBlob(), encodingType);
                content.setManifestedAs(encodingList.stream()
                        .map(encoding::deserialize)
                        .collect(Collectors.toSet()));
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
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
