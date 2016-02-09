package org.atlasapi.topic;

import java.io.IOException;

import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.EsAlias;
import org.atlasapi.util.EsObject;

import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class EsTopic extends EsObject {

    public static final XContentBuilder getMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .startObject(TYPE_NAME)
                .startObject("_all")
                .field("enabled").value("false")
                .endObject()
                .startObject("properties")
                .startObject(ID)
                .field("type").value("long")
                .endObject()
                .startObject(SOURCE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(SOURCE)
                .field("type").value("string")
                .field("index").value("not_analyzed")
                .endObject()
                .startObject(TITLE)
                .field("type").value("string")
                .field("index").value("analyzed")
                .endObject()
                .startObject(DESCRIPTION)
                .field("type").value("string")
                .field("index").value("analyzed")
                .endObject()
                .startObject(ALIASES)
                .field("type").value("nested")
                .rawField("properties", EsAlias.getMapping().bytes())
                .endObject()
                .endObject()
                .endObject()
                .endObject();
    }

    public static final String TYPE_NAME = "topic";

    public static final String ID = "id";
    public static final String TYPE = "type";
    public static final String SOURCE = "source";
    public static final String ALIASES = "aliases";
    public static final String TITLE = "title";
    public static final String DESCRIPTION = "description";

    public EsTopic id(long id) {
        properties.put(ID, id);
        return this;
    }

    public EsTopic type(Topic.Type type) {
        if (type != null) {
            properties.put(TYPE, type.key());
        }
        return this;
    }

    public EsTopic source(Publisher source) {
        properties.put(SOURCE, source.key());
        return this;
    }

    public EsTopic aliases(Iterable<Alias> aliases) {
        properties.put(
                ALIASES,
                Iterables.transform(aliases, Functions.compose(TO_MAP, EsAlias.toEsAlias()))
        );
        return this;
    }

    public EsTopic title(String title) {
        properties.put(TITLE, title);
        return this;
    }

    public EsTopic description(String desc) {
        properties.put(DESCRIPTION, desc);
        return this;
    }

}
