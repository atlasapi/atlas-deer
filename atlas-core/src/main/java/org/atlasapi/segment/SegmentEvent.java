package org.atlasapi.segment;

import java.util.Comparator;

import org.atlasapi.content.Description;
import org.atlasapi.content.Identified;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

public class SegmentEvent extends Identified {

    private Integer position;
    private Duration offset;
    private Boolean isChapter;
    
    private Description description = Description.EMPTY;
    
    private SegmentRef segment;
    
    private String versionId;
    
    @FieldName("position")
    public Integer getPosition() {
        return this.position;
    }

    @FieldName("offset")
    public Duration getOffset() {
        return this.offset;
    }

    @FieldName("is_chapter")
    public Boolean getIsChapter() {
        return this.isChapter;
    }

    @FieldName("description")
    public Description getDescription() {
        return this.description;
    }

    @FieldName("segment")
    public SegmentRef getSegmentRef() {
        return this.segment;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public void setOffset(Duration offset) {
        this.offset = offset;
    }

    public void setIsChapter(Boolean isChapter) {
        this.isChapter = isChapter;
    }

    public void setDescription(Description description) {
        this.description = description;
    }

    public void setSegment(SegmentRef segment) {
        this.segment = segment;
    }

    @FieldName("version_id")
    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public static final Ordering<SegmentEvent> ORDERING = Ordering.from(new Comparator<SegmentEvent>() {
        @Override
        public int compare(SegmentEvent s1, SegmentEvent s2) {
            if (s1.position == null || s2.position == null) {
                return 0;
            }
            return Ints.compare(s1.position, s2.position);
        }
    });

    public static final Function<SegmentEvent, SegmentRef> TO_REF = new Function<SegmentEvent, SegmentRef>() {

        @Override
        public SegmentRef apply(SegmentEvent input) {
            return input.getSegmentRef();
        }
        
    };
}