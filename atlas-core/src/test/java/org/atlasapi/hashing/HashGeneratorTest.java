package org.atlasapi.hashing;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HashGeneratorTest {

    @Mock private HashValueExtractor extractor;
    @Mock private Hashable hashable;

    private HashGenerator hashGenerator;

    @Before
    public void setUp() throws Exception {
        hashGenerator = HashGenerator.create(extractor);
    }

    @Test
    public void whenExtractReturnsEmptyGeneratorReturnsEmptyHash() throws Exception {
        when(extractor.getValueToHash(hashable)).thenReturn(Optional.empty());

        Optional<String> hash = hashGenerator.hash(hashable);

        assertThat(hash.isPresent(), is(false));
    }

    @Test
    public void hashDoesNotReturnSameValueAsExtractor() throws Exception {
        String valueToHash = "value";

        when(extractor.getValueToHash(hashable)).thenReturn(Optional.of(valueToHash));

        String hash = hashGenerator.hash(hashable).get();

        assertThat(hash, not(nullValue()));
        assertThat(hash, not(is(valueToHash)));
    }

    @Test
    public void hashingTheSameValueReturnsTheSameResult() throws Exception {
        String valueToHash = "value";

        when(extractor.getValueToHash(hashable)).thenReturn(Optional.of(valueToHash));

        String firstHash = hashGenerator.hash(hashable).get();
        String secondHash = hashGenerator.hash(hashable).get();

        assertThat(firstHash, is(secondHash));
    }
}
