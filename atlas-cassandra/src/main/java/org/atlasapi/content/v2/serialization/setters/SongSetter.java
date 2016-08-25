package org.atlasapi.content.v2.serialization.setters;

import org.atlasapi.content.Song;
import org.atlasapi.content.v2.model.Content;

import org.joda.time.Duration;

public class SongSetter {

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Song.class.isInstance(content)) {
            return;
        }

        Song song = (Song) content;
        internal.setIsrc(song.getIsrc());
        Duration duration = song.getDuration();
        if (duration != null) {
            internal.setDuration(duration.getMillis());
        }
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Song song = (Song) content;

        song.setIsrc(internal.getIsrc());
        Long duration = internal.getDuration();
        if (duration != null) {
            song.setDuration(new Duration(duration));
        }
    }
}