package org.atlasapi.content;

import java.util.List;
import java.util.Set;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.time.DateTimeZones;

public class ComplexItemTestDataBuilder {
    private static Long uniqueId = 1000L;
    
    private String uri;
    private String curie;
    
    private Set<String> aliasUrls;
    private Set<Alias> aliases;
    private Container brand;
    private List<Clip> clips;

    private MediaType mediaType;

    private String description;
    private Set<String> genres;
    private String image;
    private Publisher publisher;
    private Set<String> tags;
    private String thumbnail;
    private String title;
    private Long id;
    private Set<Version> versions;
    
    public static ComplexItemTestDataBuilder complexItem() {
        return new ComplexItemTestDataBuilder();
    }
    
    private ComplexItemTestDataBuilder() {
        uri = "http://test.metabroadcast.com/item/default";
        curie = "mbtest:i-default";
        
        id = uniqueId++;
        uri = "http://test.metabroadcast.com/unique/items/" + id;
        curie = "mbtest:i-" + id;
        
        aliasUrls = ImmutableSet.of();
        aliases = ImmutableSet.of();
        brand = null;
        clips = ImmutableList.of();
        mediaType = null;
        description = "Default test item created by ItemTestDataBuilder";
        genres = ImmutableSet.of("http://test.metabroadcast.com/genres/default");
        image = "http://test.metabroadcast.com/images/default";
        publisher = Publisher.BBC;
        tags = ImmutableSet.of();
        thumbnail = "http://test.metabroadcast.com/thumbnails/default";
        title = "Default Test Item";
        versions = ImmutableSet.of();
    }
    
    public Item build() {
        return buildItemAttributes(new Item());
    }
    
    private Item buildItemAttributes(Item item) {
        item.setAliasUrls(aliasUrls);
        item.setAliases(aliases);
        if (brand != null) {
            item.setContainer(brand);
        }
        item.setId(Id.valueOf(id));
        item.setVersions(versions);
        item.setClips(clips);
        item.setMediaType(mediaType);
        item.setMediaType(mediaType);
        item.setDescription(description);
        item.setGenres(genres);
        item.setImage(image);
        item.setPublisher(publisher);
        item.setTags(tags);
        item.setThumbnail(thumbnail);
        item.setTitle(title);
        item.setCanonicalUri(uri);
        item.setCurie(curie);
        item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        return item;
    }
    
    public ComplexItemTestDataBuilder withClips(Clip... clips) {
        this.clips = ImmutableList.copyOf(clips);
        return this;
    }
    
    public ComplexItemTestDataBuilder withTitle(String title) {
        this.title = title;
        return this;
    }
    
    public ComplexItemTestDataBuilder withUri(String uri) {
        this.uri = uri;
        return this;
    }
    
    public ComplexItemTestDataBuilder withId(Long id) {
        this.id = id;
        return this;
    }
    
    public ComplexItemTestDataBuilder withTags(String... tags) {
        this.tags = ImmutableSet.copyOf(tags);
        return this;
    }
    
    public ComplexItemTestDataBuilder withAliasUrls(String... aliases) {
        this.aliasUrls = ImmutableSet.copyOf(aliases);
        return this;
    }
    
    public ComplexItemTestDataBuilder withAliases(Alias... aliases) {
        this.aliases = ImmutableSet.copyOf(aliases);
        return this;
    }
    
    public ComplexItemTestDataBuilder withBrand(Container brand) {
        this.brand = brand;
        
        brand.setItemRefs(ImmutableSet.<ItemRef>builder().addAll(brand.getItemRefs()).add(this.build().toRef()).build());
        
        return this;
    }
    
    public ComplexItemTestDataBuilder withVersions(Version...versions) {
        this.versions = ImmutableSet.copyOf(versions);
        return this;
    }
    
    public ComplexItemTestDataBuilder withDescription(String desciption) {
        this.description = desciption;
        return this;
    }
}
