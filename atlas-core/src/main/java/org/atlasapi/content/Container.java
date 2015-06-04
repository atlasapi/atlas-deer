package org.atlasapi.content;

import com.google.common.collect.ImmutableMap;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public abstract class Container extends Content {

    protected ImmutableList<ItemRef> itemRefs = ImmutableList.of();
    private Map<ItemRef, Iterable<BroadcastRef>> upcomingContent = ImmutableMap.of();

	public Container(String uri, String curie, Publisher publisher) {
		super(uri, curie, publisher);
	}
	
    public Container(Id id, Publisher source) {
        super(id, source);
    }
    
    public Container() {}
    
    @FieldName("item_refs")
    public ImmutableList<ItemRef> getItemRefs() {
        return itemRefs;
    }
    
    public void setItemRefs(Iterable<ItemRef> itemRefs) {
        this.itemRefs = ImmutableList.copyOf(itemRefs);
    }
    
    public final static <T extends Item> void copyTo(Container from, Container to) {
        Content.copyTo(from, to);
        to.itemRefs = ImmutableList.copyOf(from.itemRefs);
    }

    public abstract <V> V accept(ContainerVisitor<V> visitor);
    
    public abstract ContainerRef toRef();

    public abstract ContainerSummary toSummary();
    
    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ContainerVisitor<V>) visitor);
    }

    public Map<ItemRef, Iterable<BroadcastRef>> getUpcomingContent() {
        return upcomingContent;
    }

    public void setUpcomingContent(Map<ItemRef, Iterable<BroadcastRef>> upcomingContent) {
        this.upcomingContent = ImmutableMap.copyOf(upcomingContent);
    }
}
