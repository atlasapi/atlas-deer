package org.atlasapi.content;

import java.util.Map;
import java.util.Optional;

import org.atlasapi.entity.Id;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.context.QueryContext;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexQueryParserTest {

    @Test
    public void testActionableFilterParamsFrom() throws Exception {
        IndexQueryParser parser = new IndexQueryParser();
        MockHttpServletRequest mockReq = new MockHttpServletRequest("GET", "test");
        mockReq.setParameters(ImmutableMap.of(
                "actionableFilterParameters",
                "location.available:true,broadcast.time.gt:201501232"
        ));

        QueryContext mock = mock(QueryContext.class);

        when(mock.getRequest()).thenReturn(mockReq);
        Query.SingleQuery<Object> query = Query.singleQuery(Id.valueOf(10l), mock);
        Optional<Map<String, String>> params = parser.actionableFilterParamsFrom(query);
        assertThat(params.isPresent(), is(equalTo(true)));
        assertThat(params.get().get("location.available"), is(equalTo("true")));
        assertThat(params.get().size(), is(equalTo(2)));
    }
}
