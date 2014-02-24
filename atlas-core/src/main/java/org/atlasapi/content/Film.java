package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;

public class Film extends Item {
    
    private String websiteUrl = null;
    private Set<Subtitles> subtitles = ImmutableSet.of();
    private Set<ReleaseDate> releaseDates = ImmutableSet.of();
    
    public Film(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
        setSpecialization(Specialization.FILM);
    }
        
    public Film(Id id, Publisher source) {
        super(id, source);
    }
    
    public Film() {
        setSpecialization(Specialization.FILM);
    }
    
    public void setWebsiteUrl(String websiteUrl) {
        this.websiteUrl = websiteUrl;
    }
    
    public String getWebsiteUrl() {
        return websiteUrl;
    }

    public Set<Subtitles> getSubtitles() {
        return subtitles;
    }

    public void setSubtitles(Iterable<Subtitles> subtitles) {
        this.subtitles = ImmutableSet.copyOf(subtitles);
    }

    public Set<ReleaseDate> getReleaseDates() {
        return releaseDates;
    }

    public void setReleaseDates(Iterable<ReleaseDate> releaseDates) {
        this.releaseDates = ImmutableSet.copyOf(releaseDates);
    }
    
    @Override
    public FilmRef toRef() {
        return new FilmRef(getId(), getPublisher(), SortKey.keyFrom(this), getThisOrChildLastUpdated());
    }

    @Override
	public Film copy() {
	    return copyTo(this, new Film());
	}
	
	public static Film copyTo(Film from, Film to) {
	    Item.copyTo(from, to);
	    to.setWebsiteUrl(from.getWebsiteUrl());
	    to.setSubtitles(from.getSubtitles());
	    to.setReleaseDates(from.getReleaseDates());
	    return to;
	}

    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }
    
}
