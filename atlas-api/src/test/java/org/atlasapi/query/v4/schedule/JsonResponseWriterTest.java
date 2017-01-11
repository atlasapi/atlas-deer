package org.atlasapi.query.v4.schedule;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.JsonResponseWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.query.common.context.QueryContext;

import com.metabroadcast.common.servlet.StubHttpServletRequest;
import com.metabroadcast.common.servlet.StubHttpServletResponse;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class JsonResponseWriterTest {

    private ObjectMapper mapper;
    private HttpServletRequest request;
    private StubHttpServletResponse response;

    private JsonResponseWriter formatter;
    private OutputContext ctxt;

    @Before
    public void setup() throws IOException {
        mapper = new ObjectMapper();
        request = new StubHttpServletRequest();
        response = new StubHttpServletResponse();
        ctxt = OutputContext.valueOf(QueryContext.standard(mock(HttpServletRequest.class)));
        formatter = new JsonResponseWriter(request, response);
        formatter.startResponse();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> asMap(StubHttpServletResponse response) throws IOException {
        return mapper.readValue(response.getResponseAsString(), Map.class);
    }

    @Test
    public void testWritingSingleField() throws Exception {

        formatter.writeField("hello", "world");
        formatter.finishResponse();

        assertEquals("world", asMap(response).get("hello"));
    }

    @Test
    public void testWritingManyFields() throws Exception {

        formatter.writeField("hello", "world");
        formatter.writeField("bonjour", "monde");
        formatter.writeField("halt", "hammerzeit");
        formatter.finishResponse();

        Map<String, Object> deser = asMap(response);

        assertEquals("world", deser.get("hello"));
        assertEquals("monde", deser.get("bonjour"));
        assertEquals("hammerzeit", deser.get("halt"));
    }

    @Test
    public void testWritingPrimitiveFields() throws Exception {

        formatter.writeField("int", 1);
        formatter.writeField("long", 4294967296L);
        formatter.writeField("double", 3.5D);
        formatter.writeField("boolean", true);
        formatter.finishResponse();

        Map<String, Object> deser = asMap(response);

        assertEquals(1, deser.get("int"));
        assertEquals(4294967296L, deser.get("long"));
        assertEquals(3.5D, deser.get("double"));
        assertEquals(true, deser.get("boolean"));
    }

    @Test
    public void testWriteEmptyResponse() throws IOException {
        formatter.finishResponse();
        assertThat(asMap(response).isEmpty(), is(true));

    }

    @Test
    public void testWritingNullFields() throws Exception {

        formatter.writeField("null", null);
        formatter.finishResponse();

        assertNull(null, asMap(response).get("null"));
    }

    @Test
    public void testWritingNestedObjects() throws Exception {

        formatter.writeObject(new EntityWriter<String>() {

            @Override
            public void write(String entity, FieldWriter formatter, OutputContext ctxt)
                    throws IOException {
                formatter.writeField("nested_field", entity);
                formatter.writeField("nested_again", entity);
            }

            @Override
            public String fieldName(String entity) {
                return "nested";
            }
        }, "value", ctxt);
        formatter.finishResponse();

        ImmutableMap<String, String> expectedMap = ImmutableMap.of(
                "nested_field", "value",
                "nested_again", "value"
        );
        assertEquals(expectedMap, asMap(response).get("nested"));
    }

    @Test
    public void testWritingEmptyArray() throws Exception {

        formatter.writeList("elems", "elem", ImmutableList.of(), ctxt);
        formatter.finishResponse();

        assertEquals(ImmutableList.of(), asMap(response).get("elems"));
    }

    @Test
    public void testWritingSingletonArray() throws Exception {

        formatter.writeList("elems", "elem", ImmutableList.of("elem"), ctxt);
        formatter.finishResponse();

        assertEquals(ImmutableList.of("elem"), asMap(response).get("elems"));
    }

    @Test
    public void testWritingRegularArray() throws Exception {

        formatter.writeList("elems", "elem", ImmutableList.of("elem", "elen", "eleo"), ctxt);
        formatter.finishResponse();

        assertEquals(ImmutableList.of("elem", "elen", "eleo"), asMap(response).get("elems"));
    }

    @Test
    public void testWritingObjectArray() throws Exception {

        formatter.writeList(new EntityListWriter<String>() {

            @Override
            public void write(String entity, FieldWriter formatter, OutputContext ctxt)
                    throws IOException {
                formatter.writeField("prop", entity);
            }

            @Override
            public String listName() {
                return "elems";
            }

            @Override
            public String fieldName(String entity) {
                return "elem";
            }

        }, ImmutableList.of("elem", "elen", "eleo"), ctxt);
        formatter.finishResponse();

        ImmutableList<ImmutableMap<String, String>> expectedList = ImmutableList.of(
                ImmutableMap.of("prop", "elem"),
                ImmutableMap.of("prop", "elen"),
                ImmutableMap.of("prop", "eleo")
        );

        assertEquals(expectedList, asMap(response).get("elems"));
    }

    @Test
    public void testEscapesCharacters() throws Exception {
        String one = "asdf\"zxcv";
        String two = "asdf\\zxcv";
        formatter.writeField("one", one);
        formatter.writeField("two", two);
        formatter.finishResponse();
        Map<String, Object> deser = asMap(response);
        assertEquals(one, deser.get("one"));
        assertEquals(two, deser.get("two"));
    }

    @Test
    public void testEscapesSpecialsCharacters() throws Exception {
        String testString = "\t\r\n\b\f\\/";
        formatter.writeField("hello", testString);
        formatter.finishResponse();

        assertEquals(testString, asMap(response).get("hello"));
    }

    @Test
    public void testDoesntWriteAnythingWhenAnExceptionIsThrown() throws IOException {
        try {
            formatter.writeObject(new EntityWriter<Integer>() {

                @Override
                public void write(Integer entity, FieldWriter writer, OutputContext ctxt)
                        throws IOException {
                    writer.writeField("error", 100 / entity);
                }

                @Override
                public String fieldName(Integer entity) {
                    return "field";
                }
            }, 0, ctxt);
        } catch (Exception e) {
            // excpected
        }
        assertTrue(Strings.isNullOrEmpty(response.getResponseAsString()));
    }

    @Test
    public void testWritingEmptyArrayFollowedByField() throws Exception {

        formatter.writeList("elems", "elem", ImmutableList.<String>of(), ctxt);
        formatter.writeField("hello", "world");
        formatter.finishResponse();

        Map<String, Object> results = asMap(response);
        assertEquals(ImmutableList.<String>of(), results.get("elems"));
        assertEquals("world", results.get("hello"));
    }
}
