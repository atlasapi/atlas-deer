package org.atlasapi.output.writers;

import org.atlasapi.content.Item;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.annotation.DescriptionAnnotation;
import org.atlasapi.output.annotation.IdentificationSummaryAnnotation;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class ItemDetailWriter implements EntityWriter<Item> {

    private final IdentificationSummaryAnnotation identificationSummaryAnnotation;
    private final DescriptionAnnotation<Item> descriptionAnnotation = new DescriptionAnnotation<>();

    public ItemDetailWriter(IdentificationSummaryAnnotation identificationSummaryAnnotation) {
        this.identificationSummaryAnnotation = checkNotNull(identificationSummaryAnnotation);
    }

    @Override
    public void write(@Nonnull Item entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException {
        identificationSummaryAnnotation.write(entity, writer, ctxt);
        descriptionAnnotation.write(entity, writer, ctxt);
    }

    @Nonnull
    @Override
    public String fieldName(Item entity) {
        return "item";
    }
}
