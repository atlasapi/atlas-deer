package org.atlasapi.content;

import com.google.common.base.Objects;

import javax.annotation.Nullable;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemSummary {

    private final ItemRef itemRef;
    private final String title;
    private final Optional<String> description;
    private final Optional<String> image;

    public ItemSummary(ItemRef itemRef, String title, @Nullable String description, @Nullable String image) {
        this.itemRef = checkNotNull(itemRef);
        this.title = checkNotNull(title);
        this.description = Optional.ofNullable(description);
        this.image = Optional.ofNullable(image);
    }

    public ItemRef getItemRef() {
        return itemRef;
    }

    public String getTitle() {
        return title;
    }

    public Optional<String> getImage() {
        return image;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(itemRef);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemSummary)) return false;
        ItemSummary that = (ItemSummary) o;
        return Objects.equal(itemRef, that.itemRef);
    }

    public Optional<String> getDescription() {
        return description;
    }
}
