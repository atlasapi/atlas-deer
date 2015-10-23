package org.atlasapi.util;

import java.util.stream.Collectors;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Specialization;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.intl.Countries;

public class ClientModelConverter {
    private final NumberToShortStringCodec codec = new SubstitutionTableNumberCodec();

    public Content convert(org.atlasapi.deer.client.model.types.Content api) {
        switch (api.getType()) {
        case "item":
            return makeItem(api);
        default:
            throw new IllegalArgumentException(
                    String.format("Unrecognised content type %s", api.getType()));
        }
    }

    private Content makeItem(org.atlasapi.deer.client.model.types.Content api) {
        Item item = new Item();

        if (api.getId() != null) {
            item.setId(codec.decode(api.getId()).longValueExact());
        }

        // TODO: immutable collector
        item.setAliases(api
                .getAliases()
                .stream()
                .map(clientAlias -> new Alias(clientAlias.getNamespace(), clientAlias.getValue()))
                .collect(Collectors.toList())
        );

        if (api.getMediaType() != null) {
            item.setMediaType(MediaType.fromKey(api.getMediaType()).get());
        }

        if (api.getSpecialization() != null) {
            item.setSpecialization(Specialization.fromKey(api.getSpecialization()).requireValue());
        }

        if (api.getSource() != null) {
            item.setPublisher(Publisher.fromKey(api.getSource().getKey()).requireValue());
        }

        item.setTitle(api.getTitle());
        item.setDescription(api.getDescription());
        item.setImage(api.getImage());
        item.setThumbnail(api.getThumbnail());
        item.setGenres(api.getGenres());
        item.setPresentationChannel(api.getPresentationChannel());
        item.setLongDescription(api.getLongDescription());
        item.setBlackAndWhite(api.getBlackAndWhite());

        // TODO: immutable collector
        if (api.getCountriesOfOrigin() != null) {
            item.setCountriesOfOrigin(
                    api.getCountriesOfOrigin()
                            .stream()
                            .map(Countries::fromCode)
                            .collect(Collectors.toSet()));
        }

        item.setScheduleOnly(api.getScheduleOnly());
        item.setLanguages(api.getLanguages());

        return item;
    }
}
