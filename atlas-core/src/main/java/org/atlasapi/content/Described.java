/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.Sourced;
import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.text.MoreStrings;

public abstract class Described extends Identified implements Sourced {

	private String title;

    private String shortDescription;
    private String mediumDescription;
    private String longDescription;
	
	private Synopses synopses;
	
	private String description;
		
	private MediaType mediaType = MediaType.VIDEO;
	private Specialization specialization;
	
	private ImmutableSet<String> genres = ImmutableSet.of();
	private Set<String> tags = Sets.newHashSet();
	
	protected Publisher publisher;
	private String image;
	private Set<Image> images = ImmutableSet.of();
	private String thumbnail;
	
	private DateTime firstSeen;
	private DateTime lastFetched;
	private DateTime thisOrChildLastUpdated;
	private boolean scheduleOnly = false;
    private boolean activelyPublished = true;

    private String presentationChannel;

    protected Set<RelatedLink> relatedLinks = ImmutableSet.of();
	
	public Described(String uri, String curie, Publisher publisher) {
		super(uri, curie);
		this.publisher = publisher;
	}
	
	public Described(String uri, String curie) {
		this(uri, curie, null);
	}
	
	public Described(String uri) {
		super(uri);
	}
	
	public Described() { /* some legacy code still requires a default constructor */ }
	
	public Described(Id id, Publisher source) {
	    super(id);
	    this.publisher = source;
	}

    public DateTime getLastFetched() {
        return lastFetched;
    }

    public void setLastFetched(DateTime lastFetched) {
        this.lastFetched = lastFetched;
    }
    
    public DateTime getFirstSeen() {
        return this.firstSeen;
    }

    public void setFirstSeen(DateTime firstSeen) {
        this.firstSeen = firstSeen;
    }
    
    public void setGenres(Iterable<String> genres) {
        this.genres = ImmutableSet.copyOf(genres);
    }

	public Set<String> getGenres() {
		return this.genres;
	}
	
	public void setTitle(String title) {
		this.title = title;
	}

    public Synopses getSynopses() {
        return this.synopses;
    }

	public String getDescription() {
		return this.description;
	}

    public void setSynopses(Synopses synopses) {
        this.synopses = synopses;
    }

	public void setDescription(String description) {
		this.description = description;
	}

    public String getShortDescription() {
        return this.shortDescription;
    }

    public String getMediumDescription() {
        return this.mediumDescription;
    }

    public String getLongDescription() {
        return this.longDescription;
    }

    public void setShortDescription(String shortDescription) {
        this.shortDescription = shortDescription;
    }

    public void setMediumDescription(String mediumDescription) {
        this.mediumDescription = mediumDescription;
    }
    
    public void setLongDescription(String longDescription) {
        this.longDescription = longDescription;
    }

	public Set<String> getTags() {
		return tags;
	}
	
	public void setTags(Set<String> tags) {
	    if (tags != null && ! tags.isEmpty()) {
	        this.tags = Sets.newHashSet(Iterables.transform(tags, MoreStrings.TO_LOWER));
	    } else {
	        this.tags = tags;
	    }
	}
	
	public Publisher getPublisher() {
		return publisher;
	}

	public void setPublisher(Publisher publisher) {
		this.publisher = publisher;
	}
	    
    public String getImage() {
		return image;
	}
    
    public String getThumbnail() {
		return thumbnail;
	}
    
    public void setImage(String image) {
		this.image = image;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}

	public String getTitle() {
		return this.title;
	}
	
	public DateTime getThisOrChildLastUpdated() {
        return thisOrChildLastUpdated;
    }
	
	public void setThisOrChildLastUpdated(DateTime thisOrChildLastUpdated) {
        this.thisOrChildLastUpdated = thisOrChildLastUpdated;
    }
	
	public boolean isActivelyPublished() {
	    return activelyPublished;
	}
	
	public void setActivelyPublished(boolean activelyPublished) {
	    this.activelyPublished = activelyPublished;
	}
	
	public void setMediaType(MediaType mediaType) {
		this.mediaType = mediaType;
	}
	
	public MediaType getMediaType() {
		return this.mediaType;
	}
	
	public Specialization getSpecialization() {
        return specialization;
    }

    public void setSpecialization(Specialization specialization) {
        this.specialization = specialization;
    }
    
    public void setScheduleOnly(boolean scheduleOnly) {
        this.scheduleOnly = scheduleOnly;
    }
    
    public boolean isScheduleOnly() {
        return scheduleOnly;
    }
    
    public void setPresentationChannel(Channel channel) {
        setPresentationChannel(channel.getKey());
    }
    
    public void setPresentationChannel(String channel) {
        this.presentationChannel = channel;
    }
    
    public String getPresentationChannel() {
        return this.presentationChannel;
    }
    
    public void setImages(Iterable<Image> images) {
        this.images = ImmutableSet.copyOf(images);
    }
    
    public Set<Image> getImages() {
        return images;
    }
    
    public Image getPrimaryImage() {
        return Iterables.getOnlyElement(Iterables.filter(images, Image.IS_PRIMARY), null);
    }
    
    public Set<RelatedLink> getRelatedLinks() {
        return relatedLinks;
    }

    public void setRelatedLinks(Iterable<RelatedLink> links) {
        relatedLinks = ImmutableSet.copyOf(links);
    }

    public void addRelatedLink(RelatedLink link) {
        relatedLinks = ImmutableSet.<RelatedLink>builder().add(link).addAll(relatedLinks).build();
    }
    
    public static void copyTo(Described from, Described to) {
        Identified.copyTo(from, to);
        to.description = from.description;
        to.firstSeen = from.firstSeen;
        to.genres = ImmutableSet.copyOf(from.genres);
        to.image = from.image;
        to.lastFetched = from.lastFetched;
        to.mediaType = from.mediaType;
        to.publisher = from.publisher;
        to.specialization = from.specialization;
        to.tags = Sets.newHashSet(from.tags);
        to.thisOrChildLastUpdated = from.thisOrChildLastUpdated;
        to.thumbnail = from.thumbnail;
        to.title = from.title;
        to.scheduleOnly = from.scheduleOnly;
        to.presentationChannel = from.presentationChannel;
        to.images = from.images;
        to.shortDescription = from.shortDescription;
        to.mediumDescription = from.mediumDescription;
        to.longDescription = from.longDescription;
    }
    
    public abstract Described copy();

    public <T extends Described> boolean isEquivalentTo(T content) {
        return getEquivalentTo().contains(EquivalenceRef.valueOf(content))
            || Iterables.contains(Iterables.transform(content.getEquivalentTo(), Identifiables.toId()), getId());
    }
    
}
