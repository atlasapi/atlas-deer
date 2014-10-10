package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.google.common.collect.ImmutableList;

public abstract class Container extends Content {

    protected ImmutableList<ItemRef> itemRefs = ImmutableList.of();

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
    
    @Override
    public <V> V accept(ContentVisitor<V> visitor) {
        return accept((ContainerVisitor<V>) visitor);
    }
    
}
