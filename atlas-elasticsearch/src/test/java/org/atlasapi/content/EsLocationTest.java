package org.atlasapi.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.atlasapi.entity.Alias;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EsLocationTest {

    private static final String AVAILABILITY_TIME = "availabilityTime";
    private static final String AVAILABILITY_END_TIME = "availabilityEndTime";
    private static final String ALIASES = "aliases";
    private static final String NAMESPACE = "namespace";
    private static final String VALUE = "value";

    private static final String DATE_A = "16/08/2016 13:14:15";
    private static final String DATE_B = "17/08/2016 14:15:16";

    private DateTimeFormatter formatter;

    private Map<String, Object> locationMap;

    private Map<String, Object> expectedMap = new HashMap<>();

    private Alias aliasA, aliasB;
    private List<Alias> aliases = new ArrayList<>();

    @Before
    public void setUp() {
        formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");

        aliasA = new Alias("namespaceA", "valueA");
        aliasB = new Alias("namespaceB", "valueB");
        aliases.add(aliasA);
        aliases.add(aliasB);

        Map<String, String> aliasAMap = ImmutableMap.of(
                "namespace", "namespaceA",
                "value", "valueA"
        );
        Map<String, String> aliasBMap = ImmutableMap.of(
                "namespace", "namespaceB",
                "value", "valueB"
        );

        locationMap = ImmutableMap.of(
                AVAILABILITY_TIME, formatter.parseDateTime(DATE_A),
                AVAILABILITY_END_TIME, formatter.parseDateTime(DATE_B),
                ALIASES, Lists.newArrayList(aliasAMap, aliasBMap)
        );

        EsLocation expectedEsLocation = new EsLocation()
                .availabilityTime(formatter.parseDateTime(DATE_A).toDate())
                .availabilityEndTime(formatter.parseDateTime(DATE_B).toDate())
                .aliases(aliases);

        expectedMap = expectedEsLocation.toMap();
    }

    @Test
    public void addAliasestoEsLocation() {

        Map<String, Object> actualMap = new EsLocation().aliases(aliases).toMap();

        Iterator<Map<String,Object>> actualAliasesIterator =
                ((Iterable<Map<String,Object>>) actualMap.get(ALIASES)).iterator();

        Map<String, Object> actualAliasMapA = actualAliasesIterator.next();
        Map<String, Object> actualAliasMapB = actualAliasesIterator.next();

        assertThat(actualAliasMapA.get(NAMESPACE), is(aliasA.getNamespace()));
        assertThat(actualAliasMapA.get(VALUE), is(aliasA.getValue()));

        assertThat(actualAliasMapB.get(NAMESPACE), is(aliasB.getNamespace()));
        assertThat(actualAliasMapB.get(VALUE), is(aliasB.getValue()));

    }

    @Test
    public void addAvailibilityTimesToEsLocation() {
        EsLocation actualEsLocation = new EsLocation();

        actualEsLocation.availabilityTime(formatter.parseDateTime(DATE_A).toDate());
        Map<String, Object> actualMapA = actualEsLocation.toMap();

        assertThat(actualMapA.get(AVAILABILITY_TIME),
                is(expectedMap.get(AVAILABILITY_TIME)));

        actualEsLocation.availabilityEndTime(formatter.parseDateTime(DATE_B).toDate());
        Map<String, Object> actualMapB = actualEsLocation.toMap();

        assertThat(actualMapB.get(AVAILABILITY_TIME),
                is(expectedMap.get(AVAILABILITY_TIME)));
        assertThat(actualMapB.get(AVAILABILITY_END_TIME),
                is(expectedMap.get(AVAILABILITY_END_TIME)));
    }

    @Test
    public void createEsLocationFromMap() throws Exception {

        Map<String, Object> actualMap = EsLocation.fromMap(locationMap).toMap();

        Iterator<Map<String,Object>> actualAliasesIterator =
                ((Iterable<Map<String,Object>>) actualMap.get(ALIASES)).iterator();

        Map<String, Object> actualAliasMapA = actualAliasesIterator.next();
        Map<String, Object> actualAliasMapB = actualAliasesIterator.next();

        assertThat(actualMap.get(AVAILABILITY_TIME),
                is(expectedMap.get(AVAILABILITY_TIME)));

        assertThat(actualMap.get(AVAILABILITY_END_TIME),
                is(expectedMap.get(AVAILABILITY_END_TIME)));

        assertThat(actualAliasMapA.get(NAMESPACE), is(aliasA.getNamespace()));
        assertThat(actualAliasMapA.get(VALUE), is(aliasA.getValue()));

        assertThat(actualAliasMapB.get(NAMESPACE), is(aliasB.getNamespace()));
        assertThat(actualAliasMapB.get(VALUE), is(aliasB.getValue()));
    }
}
