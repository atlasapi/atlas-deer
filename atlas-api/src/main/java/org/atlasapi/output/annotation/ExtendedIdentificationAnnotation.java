package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.entity.Identified;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.AliasWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class ExtendedIdentificationAnnotation extends OutputAnnotation<Identified> {

    private final EntityListWriter<EquivalenceRef> equivalentWriter = new EntityListWriter<EquivalenceRef>() {

        @Override
        public void write(EquivalenceRef entity, FieldWriter formatter, OutputContext ctxt)
                throws IOException {
            formatter.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
            formatter.writeField("source", entity.getSource().key());
        }

        @Override
        public String listName() {
            return "same_as";
        }

        @Override
        public String fieldName(EquivalenceRef entity) {
            return "equivalent";
        }
    };

    private final NumberToShortStringCodec idCodec;
    private final AliasWriter aliasWriter;

    public ExtendedIdentificationAnnotation(NumberToShortStringCodec idCodec) {
        super();
        this.idCodec = idCodec;
        this.aliasWriter = new AliasWriter();
    }

    @Override
    public void write(Identified entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeList(aliasWriter, entity.getAliases(), ctxt);
        writer.writeList(equivalentWriter, entity.getEquivalentTo(), ctxt);
    }

}
