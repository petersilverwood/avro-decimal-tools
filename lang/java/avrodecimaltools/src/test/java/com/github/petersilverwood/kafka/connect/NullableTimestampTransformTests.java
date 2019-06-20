package com.github.petersilverwood.kafka.connect;


import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.time.Instant;
import java.util.*;


public class NullableTimestampTransformTests {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;

    static{
        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);

        DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
    }

    private final NullableTimestampTransform<SourceRecord> castNullableTimetampTransform = new NullableTimestampTransform<>();

    @Test(expected = ConfigException.class)
    public void testFieldConfigEmpty() {
        castNullableTimetampTransform.configure(Collections.singletonMap(NullableTimestampTransform.ConfigNames.FIELD, ""));
    }

    @Test
    public void castFieldsWithSchemaEpoch() {
        testFieldConversionValue(0l, java.util.Date.from(Instant.EPOCH));
    }

    @Test
    public void castFieldsWithSchemaNow() {
        testFieldConversionValue(DATE_PLUS_TIME_UNIX, DATE_PLUS_TIME.getTime());
    }

    @Test
    public void castFieldsWithSchemaNull() {
        testFieldConversionValue(null, null);
    }



    private void testFieldConversionValue(Object inputValue, java.util.Date expectedValue){

        // Configure a transform:
        Map<String, String> props = new HashMap<>();
        props.put(NullableTimestampTransform.ConfigNames.FIELD, "fld1");

        castNullableTimetampTransform.configure(props);

        // Build a sample record schema:
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("fld1", Schema.OPTIONAL_INT64_SCHEMA);
        Schema sampleSchema = builder.build();

        // Now populate a Struct with test values:
        Struct recordValue = new Struct(sampleSchema);
        recordValue.put("fld1", inputValue);

        // Apply the transformation:
        SourceRecord transformed = castNullableTimetampTransform.apply(new SourceRecord(null, null, "topic", 0,
                sampleSchema, recordValue));

        // Make assertions about the new value:
        assertEquals( expectedValue, (java.util.Date) ((Struct) transformed.value()).get("fld1"));

        // Make assertions about the new schema:
        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Timestamp.builder().optional().schema(), transformedSchema.field("fld1").schema());


    }

}
