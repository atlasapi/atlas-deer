package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;
import org.joda.time.DateTime;

import com.google.common.collect.ComparisonChain;

public class ItemRef extends ContentRef implements Comparable<ItemRef> {

    private final String sortKey;
    private final DateTime updated;

    public ItemRef(Id id, Publisher source, String sortKey, DateTime updated) {
        super(id, source);
        this.sortKey = checkNotNull(sortKey);
        this.updated = checkNotNull(updated);
    }

    @Override
    public ContentType getContentType() {
        return ContentType.ITEM;
    }
    
    @FieldName("sort_key")
    public String getSortKey() {
        return sortKey;
    }
    
    @FieldName("updated")
    public DateTime getUpdated() {
        return updated;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ItemRef) {
            ItemRef other = (ItemRef) that;
            return id.equals(other.id)
                && source.equals(other.source)
                && getContentType().equals(other.getContentType());
        }
        return false;
    }

    @Override
    public int compareTo(ItemRef other) {
          return ComparisonChain.start()
              .compare(sortKey, other.sortKey)
              .compare(id, other.id)
              .result();
    }

}
