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
        if (api.getAliases() != null) {
            item.setAliases(api
                            .getAliases()
                            .stream()
                            .map(clientAlias -> new Alias(clientAlias.getNamespace(), clientAlias.getValue()))
                            .collect(Collectors.toList())
            );
        }

        if (api.getMediaType() != null) {
            item.setMediaType(MediaType.fromKey(api.getMediaType()).get());
        }

        if (api.getSpecialization() != null) {
            item.setSpecialization(Specialization.fromKey(api.getSpecialization()).requireValue());
        }

        if (api.getSource() != null) {
            item.setPublisher(Publisher.fromKey(api.getSource().getKey()).requireValue());
        }

        if (api.getTitle() != null) {
            item.setTitle(api.getTitle());
        }

        if (api.getDescription() != null) {
            item.setDescription(api.getDescription());
        }

        if (api.getImage() != null) {
            item.setImage(api.getImage());
        }

        if (api.getThumbnail() != null) {
            item.setThumbnail(api.getThumbnail());
        }

        if (api.getGenres() != null) {
            item.setGenres(api.getGenres());
        }

        if (api.getPresentationChannel() != null) {
            item.setPresentationChannel(api.getPresentationChannel());
        }

        if (api.getLongDescription() != null) {
            item.setLongDescription(api.getLongDescription());
        }

        if (api.getBlackAndWhite() != null) {
            item.setBlackAndWhite(api.getBlackAndWhite());
        }

        // TODO: immutable collector
        if (api.getCountriesOfOrigin() != null) {
            item.setCountriesOfOrigin(
                    api.getCountriesOfOrigin()
                            .stream()
                            .map(Countries::fromCode)
                            .collect(Collectors.toSet()));
        }

        if (api.getScheduleOnly() != null) {
            item.setScheduleOnly(api.getScheduleOnly());
        }

        if (api.getLanguages() != null) {
            item.setLanguages(api.getLanguages());
        }

        return item;
    }
}
