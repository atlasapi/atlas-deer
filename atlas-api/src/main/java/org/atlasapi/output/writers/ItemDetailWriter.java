package org.atlasapi.output.writers;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.atlasapi.content.Item;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.DescriptionAnnotation;
import org.atlasapi.output.annotation.IdentificationSummaryAnnotation;
import org.atlasapi.output.annotation.LocationsAnnotation;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemDetailWriter implements EntityListWriter<Item> {

    private final IdentificationSummaryAnnotation identificationSummaryAnnotation;
    private final DescriptionAnnotation<Item> descriptionAnnotation = new DescriptionAnnotation<>();
    private final Optional<LocationsAnnotation> locationsAnnotation;
    private final String listName;

    public ItemDetailWriter(IdentificationSummaryAnnotation identificationSummaryAnnotation) {
        this(identificationSummaryAnnotation, "items", null);
    }

    public ItemDetailWriter(IdentificationSummaryAnnotation identificationSummaryAnnotation,
            String listName, LocationsAnnotation locationsAnnotation) {
        this.identificationSummaryAnnotation = checkNotNull(identificationSummaryAnnotation);
        this.listName = checkNotNull(listName);
        this.locationsAnnotation = Optional.ofNullable(locationsAnnotation);
    }

    @Override
    public void write(@Nonnull Item entity, @Nonnull FieldWriter writer,
            @Nonnull OutputContext ctxt) throws IOException {
        identificationSummaryAnnotation.write(entity, writer, ctxt);
        descriptionAnnotation.write(entity, writer, ctxt);
        if (locationsAnnotation.isPresent()) {
            locationsAnnotation.get().write(entity, writer, ctxt);
        }
    }

    @Nonnull
    @Override
    public String fieldName(Item entity) {
        return "item";
    }

    @Nonnull
    @Override
    public String listName() {
        return listName;
    }
}
