package org.atlasapi.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.content.Specialization;
import org.atlasapi.deer.client.model.types.Source;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;

public class ClientModelConverterTest {

    private ClientModelConverter converter;

    @Before
    public void setUp() {
        converter = new ClientModelConverter();
    }

    @Test
    public void convertsContent() {
        /*
          Still inconsistent:
            DisplayTitle displayTitle, -- doesn't really exist on Content, is calculated
            String priority, -- it's a compound object, not a String
            List<String> certificates, -- has a country too
            List<Restriction> restrictions, -- they're Identifieds, need extra fields
            List<Broadcast> broadcasts -- needs a channel ref, not just time
         */
        NumberToShortStringCodec codec = new SubstitutionTableNumberCodec();
        String clientId = "d9jntf";
        ImmutableList<org.atlasapi.deer.client.model.types.Alias> clientAliases =
                ImmutableList.of(
                        new org.atlasapi.deer.client.model.types.Alias("ns", "val1"),
                        new org.atlasapi.deer.client.model.types.Alias("ns", "val2")
                );
        String mediaType = "video";
        Specialization clientSpecialisation = Specialization.TV;
        Publisher clientSource = Publisher.BBC;
        String clientTitle = "item title";
        String clientDescription = "item description";
        String clientImage = "item image";
        String clientThumbnail = "item thumbnail";
        List<String> clientGenres = ImmutableList.of("genre 1", "genre 2");
        String clientPresentationChannel = "item presentation channel";
        String clientLongDescription = "item long description";
        Boolean clientScheduleOnly = Boolean.TRUE;
        List<String> clientLanguages = ImmutableList.of("lang 1", "lang 2");
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("item")
                        .id(clientId)
                        .aliases(clientAliases)
                        .mediaType(mediaType)
                        .specialization(clientSpecialisation.toString())
                        .source(new Source(clientSource.key(), clientSource.name(),
                                clientSource.country().code()))
                        .title(clientTitle)
                        .description(clientDescription)
                        .image(clientImage)
                        .thumbnail(clientThumbnail)
                        .genres(clientGenres)
                        .presentationChannel(clientPresentationChannel)
                        .longDescription(clientLongDescription)
                        .scheduleOnly(clientScheduleOnly)
                        .languages(clientLanguages)
                        .build();

        Content content = converter.convert(incoming);
        assertEquals(codec.decode(clientId), content.getId().toBigInteger());
        List<Alias> aliases = Lists.newArrayList(content.getAliases());
        assertEquals(ImmutableList.of(new Alias("ns", "val1"), new Alias("ns", "val2")), aliases);
        assertEquals(mediaType, content.getMediaType().toKey());
        assertEquals(clientSpecialisation, content.getSpecialization());
        assertEquals(clientSource, content.getSource());
        assertEquals(clientTitle, content.getTitle());
        assertEquals(clientDescription, content.getDescription());
        assertEquals(clientImage, content.getImage());
        assertEquals(clientThumbnail, content.getThumbnail());
        assertEquals(Sets.newHashSet(clientGenres), content.getGenres());
        assertEquals(clientPresentationChannel, content.getPresentationChannel());
        assertEquals(clientLongDescription, content.getLongDescription());
        assertEquals(clientScheduleOnly, content.isScheduleOnly());
        assertEquals(Sets.newHashSet(clientLanguages.iterator()), content.getLanguages());
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsContainers() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("brand")
                        .build();
        Container container = (Container) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsItems() {
        Boolean clientBlackAndWhite = Boolean.TRUE;
        List<String> clientCountriesOfOrigin = ImmutableList.of(Countries.GB.code());
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("item")
                        .blackAndWhite(clientBlackAndWhite)
                        .countriesOfOrigin(clientCountriesOfOrigin)
                        .build();

        Content content = converter.convert(incoming);
        Item item = (Item)content;
        assertEquals(clientBlackAndWhite, item.getBlackAndWhite());
        assertEquals(
                Sets.newHashSet(clientCountriesOfOrigin),
                item.getCountriesOfOrigin().stream().map(Country::code).collect(Collectors.toSet()));
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsBrands() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("brand")
                        .build();
        Brand content = (Brand) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsClips() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("clip")
                        .build();
        Clip content = (Clip) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsEpisodes() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("episode")
                        .build();
        Episode content = (Episode) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsFilms() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("episode")
                        .build();
        Film content = (Film) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsSeries() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("episode")
                        .build();
        Series content = (Series) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }

    @Ignore("Needs full mapping impl, including fixing the deer-client data objects")
    @Test
    public void convertsSongs() {
        org.atlasapi.deer.client.model.types.Content incoming =
                org.atlasapi.deer.client.model.types.Content.builder()
                        .type("episode")
                        .build();
        Song content = (Song) converter.convert(incoming);
        fail("Needs a bunch of fields");
    }
}