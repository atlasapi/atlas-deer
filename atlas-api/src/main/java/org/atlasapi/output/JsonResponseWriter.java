package org.atlasapi.output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.net.HttpHeaders;
import com.metabroadcast.common.media.MimeType;

/**
 *
 * This class is not thread-safe.
 */
public final class JsonResponseWriter implements ResponseWriter {
    
    //temporary.
    private static final ObjectMapper stringSerializer = new ObjectMapper(); 

    //private static final char STRING_DELIMITER = '"';
    private static final String NULL_VALUE = "null";
    private static final char PAIR_SEPARATOR = ':';
    private static final char ELEMENT_SEPARTOR = ',';
    private static final char MEMBER_SEPARATOR = ',';
    private static final char START_ARRAY = '[';
    private static final char END_ARRAY = ']';
    private static final char START_OBJECT = '{';
    private static final char END_OBJECT = '}';

    private static final String GZIP_HEADER_VALUE = "gzip";
    public static final String CALLBACK = "callback";

    private final HttpServletResponse response;
    private final HttpServletRequest request;
    
    private Writer writer;
    private OutputStream out;
    private ByteArrayOutputStream buffer;

    private boolean printMemberSeparator = false;
    private String callback;

    public JsonResponseWriter(HttpServletRequest request, HttpServletResponse response) {
        this.request = request;
        this.response = response;
    }
    
    private Writer writer() throws IOException {
        out = buffer = new ByteArrayOutputStream();
        String accepts = request.getHeader(HttpHeaders.ACCEPT_ENCODING);
        if (accepts != null && accepts.contains(GZIP_HEADER_VALUE)) {
            response.setHeader(HttpHeaders.CONTENT_ENCODING, GZIP_HEADER_VALUE);
            out = new GZIPOutputStream(out);
        }
        return new OutputStreamWriter(out, Charsets.UTF_8);
    }
    
    private String callback(HttpServletRequest request) {
        if (request == null) {
            return null;
        }
        String callback = request.getParameter(CALLBACK);
        if (Strings.isNullOrEmpty(callback)) {
            return null;
        }

        try {
            return URLEncoder.encode(callback, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }
    
    @Override
    public void startResponse() throws IOException {
        printMemberSeparator = false;
        response.setContentType(MimeType.APPLICATION_JSON.toString());
        writer = writer();
        callback = callback(request);
        if (callback != null) {
            writer.write(callback + "(");
        }
        writer.write(START_OBJECT);
    }
    
    @Override
    public void finishResponse() throws IOException {
        writer.write(END_OBJECT);
        if (callback != null) {
            writer.write(")");
        }
        writer.flush();
        if (out instanceof GZIPOutputStream) {
            ((GZIPOutputStream)out).finish();
        }
        response.getOutputStream().write(buffer.toByteArray());
    }

    @Override
    public void writeField(String field, Object obj) throws IOException {
        startField(field);
        if (obj != null) {
            writeString(obj.toString());
        } else {
            writeNullValue();
        }
        printMemberSeparator = true;
    }

    private void writeNullValue() throws IOException {
        writer.write(NULL_VALUE);
    }

    @Override
    public <T> void writeObject(EntityWriter<? super T> objWriter, T obj, OutputContext ctxt) throws IOException {
        startField(objWriter.fieldName(obj));
        if (obj != null) {
            writeObj(objWriter, obj, ctxt);
        } else {
            writeNullValue();
        }
        printMemberSeparator = true;
    }
    
    @Override
    public void writeList(String field, String elem, Iterable<?> list, OutputContext ctxt)
        throws IOException {
        startField(field);
        writer.write(START_ARRAY);
        
        Iterator<?> iter = list.iterator();
        if (iter.hasNext()) {
            writeString(iter.next().toString());
            while (iter.hasNext()) {
                writer.write(ELEMENT_SEPARTOR);
                writeString(iter.next().toString());
            }
        }
        writer.write(END_ARRAY);
    }

    @Override
    public <T> void writeList(EntityListWriter<? super T> listWriter, Iterable<T> list, OutputContext ctxt) throws IOException {
        startField(listWriter.listName());
        writer.write(START_ARRAY);
        
        EntityWriter<? super T> entWriter = listWriter;
        Iterator<T> iter = list.iterator();
        if (iter.hasNext()) {
            writeObj(entWriter, iter.next(), ctxt);
            while (iter.hasNext()) {
                writer.write(ELEMENT_SEPARTOR);
                writeObj(entWriter, iter.next(), ctxt);
            }
        }
        writer.write(END_ARRAY);
    }

    private <T> void writeObj(EntityWriter<? super T> entWriter, T nextObj, OutputContext ctxt) throws IOException {
        printMemberSeparator = false;
        writer.write(START_OBJECT);
        entWriter.write(nextObj, this, ctxt);
        writer.write(END_OBJECT);
        printMemberSeparator = true;
    }

    private void writeString(String string) throws IOException {
        //writer.write(STRING_DELIMITER);
        writer.write(escape(string));
        //writer.write(STRING_DELIMITER);
    }
    
    private String escape(String string) throws IOException {
        return stringSerializer.writeValueAsString(string);
    }

    private void startField(String fieldName) throws IOException {
        if (printMemberSeparator) {
            writer.write(MEMBER_SEPARATOR);
        }
        writeString(fieldName);
        writer.write(PAIR_SEPARATOR);
    }

}
