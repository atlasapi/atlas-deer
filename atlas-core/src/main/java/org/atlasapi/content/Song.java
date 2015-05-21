package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.Duration;

public class Song extends Item {

	private String isrc;
    private Long duration;
	
    public Song() {
        setMediaType(MediaType.AUDIO);
        setSpecialization(Specialization.MUSIC);
    }

    public Song(String uri, String curie, Publisher publisher) {
		super(uri, curie, publisher);
        setMediaType(MediaType.AUDIO);
        setSpecialization(Specialization.MUSIC);
	}
    
    public Song(Id id, Publisher source) {
        super(id, source);
        setMediaType(MediaType.AUDIO);
        setSpecialization(Specialization.MUSIC);
    }
	
	public void setIsrc(String isrc) {
		this.isrc = isrc;
	}

	@FieldName("isrc")
    public String getIsrc() {
        return isrc;
    }

    public void setDuration(Duration duration) {
        this.duration = duration != null ? duration.getStandardSeconds() : null;
    }

    @FieldName("duration")
    public Duration getDuration() {
        return duration != null ? Duration.standardSeconds(duration) : null;
    }
    
    @Override
    public SongRef toRef() {
        return new SongRef(getId(), getSource(), SortKey.keyFrom(this), getThisOrChildLastUpdated());
    }
	
	@Override
	public Song copy() {
	    Song song = new Song();
	    Item.copyTo(this, song);
	    song.isrc = isrc;
        song.duration = duration;
	    return song;
	}
	
    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }
    
}
