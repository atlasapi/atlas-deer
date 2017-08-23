package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.content.Actor;
import org.atlasapi.content.Content;
import org.atlasapi.content.CrewMember;
import org.atlasapi.content.ResolvedContent;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class PeopleAnnotation extends OutputAnnotation<Content, ResolvedContent> {

    public PeopleAnnotation() {
        super();
    }

    @Override
    public void write(ResolvedContent entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(new EntityListWriter<CrewMember>() {

            @Override
            public void write(CrewMember entity, FieldWriter writer, OutputContext ctxt)
                    throws IOException {
                writer.writeField("uri", entity.getCanonicalUri());
                writer.writeField("curie", entity.getCurie());
                writer.writeField("type", "person");
                writer.writeList("aliases", "alias", entity.getAliases(), ctxt);
                writer.writeField("name", entity.name());
                if (entity instanceof Actor) {
                    writer.writeField("character", ((Actor) entity).character());
                }

                // CrewMember.role() can be null
                String role = null;
                String displayRole = null;
                if (null != entity.role()) {
                    role = entity.role().key();
                    displayRole = entity.role().title();
                }

                writer.writeField("role", role);
                writer.writeField("display_role", displayRole);
            }

            @Override
            public String listName() {
                return "people";
            }

            @Override
            public String fieldName(CrewMember entity) {
                return "person";
            }
        }, entity.getContent().people(), ctxt);
    }

}
