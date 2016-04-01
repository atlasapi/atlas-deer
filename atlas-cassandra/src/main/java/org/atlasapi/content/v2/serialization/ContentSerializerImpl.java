package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Encoding;
import org.atlasapi.content.v2.model.Content;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public class ContentSerializerImpl implements ContentSerializer {

    private static final ObjectMapper mapper;
    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
    }

    private final ContentSetter contentSetter = new ContentSetter();
    private final ClipSerialization clip = new ClipSerialization();
    private final EncodingSerialization encoding = new EncodingSerialization();
    private final ContainerSetter containerSetter = new ContainerSetter();
    private final ItemSetter itemSetter = new ItemSetter();
    private final EpisodeSetter episodeSetter = new EpisodeSetter();
    private final SeriesSetter seriesSetter = new SeriesSetter();
    private final BrandSetter brandSetter = new BrandSetter();
    private final FilmSetter filmSetter = new FilmSetter();
    private final SongSetter songSetter = new SongSetter();

    @Override
    public Iterable<Content> serialize(org.atlasapi.content.Content content) {
        ImmutableList.Builder<Content> result = ImmutableList.builder();

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

        result.add(internal);

        List<org.atlasapi.content.v2.model.udt.Clip> clips = content.getClips()
                .stream()
                .map(clip::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        try {
            String json = mapper.writeValueAsString(clips);

            Content clipsJson = new Content();
            clipsJson.setId(content.getId().longValue());
            clipsJson.setDiscriminator(Content.ROW_CLIPS);
            clipsJson.setJsonBlob(json);

            result.add(clipsJson);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }

        Set<Encoding> manifestedAs = content.getManifestedAs();
        if (manifestedAs != null) {
            List<org.atlasapi.content.v2.model.udt.Encoding> encodings = manifestedAs
                    .stream()
                    .map(encoding::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            try {
                String json = mapper.writeValueAsString(encodings);

                Content encodingsJson = new Content();
                encodingsJson.setId(content.getId().longValue());
                encodingsJson.setDiscriminator(Content.ROW_ENCODINGS);
                encodingsJson.setJsonBlob(json);

                result.add(encodingsJson);
            } catch (JsonProcessingException e) {
                throw Throwables.propagate(e);
            }
        }

        return result.build();
    }

    private void setType(Content internal, org.atlasapi.content.Content content) {
        // TODO: fix this
        internal.setType(content.getClass().getSimpleName().toLowerCase());
        internal.setDiscriminator(Content.ROW_MAIN);
    }
}
