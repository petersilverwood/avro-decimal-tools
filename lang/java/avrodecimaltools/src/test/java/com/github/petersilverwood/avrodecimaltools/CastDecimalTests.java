package com.github.petersilverwood.avrodecimaltools;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CastDecimalTests {

    private final CastDecimal<SourceRecord> castDecimalTransform = new CastDecimal<>();

    @Test(expected = ConfigException.class)
    public void testFieldConfigEmpty() {
        castDecimalTransform.configure(Collections.singletonMap(CastDecimal.ConfigNames.FIELD, ""));
    }

    @Test(expected = ConfigException.class)
    public void testScaleConfigEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put(CastDecimal.ConfigNames.SCALE, "");
        props.put(CastDecimal.ConfigNames.FIELD, "fld1");

        castDecimalTransform.configure(props);
    }


    @Test
    public void testDecimalConversion() {
        BigDecimal dec = new BigDecimal("12345678910.1234");
        byte[] serialisedDec = castDecimalTransform.toBytes(dec);

        assertEquals( "12345678910.1234", castDecimalTransform.fromBytes(serialisedDec, 4).toPlainString());
    }


    @Test
    public void testDecimalConversion2() {
        BigDecimal dec = new BigDecimal("12345678910.1234");

        byte[] bufferData = dec.unscaledValue().toByteArray();
        System.out.println(bytesToHex(bufferData));

        byte[] serialisedDec = castDecimalTransform.toBytes(dec);
        assertEquals( "12345678910.1234", castDecimalTransform.fromBytes(serialisedDec, 4).toPlainString());
    }

    @Test
    public void testBytesToDecimalConversion(){

        String hexBytes = "7048860F3AB2";
        BigInteger expectedValue = new BigInteger("123456789101234");

        BigInteger actual = new BigInteger(hexBytes, 16);

        assertEquals(expectedValue, actual);


    }

    @Test
    public void testDecodeCSharpMaxValue(){

        byte[] hexBytes = decodeHexString("00FFFFFFFFFFFFFFFFFFFFFFFF");
        BigDecimal expectedValue = new BigDecimal("79228162514264337593543950335");

        BigDecimal actual = castDecimalTransform.fromBytes(hexBytes, 0);
        assertEquals(expectedValue, actual);


    }

    @Test
    public void testDecodeCSharpMinValue(){

        byte[] hexBytes = decodeHexString("FF000000000000000000000001");
        BigDecimal expectedValue = new BigDecimal("-79228162514264337593543950335");

        BigDecimal actual = castDecimalTransform.fromBytes(hexBytes, 0);
        assertEquals(expectedValue, actual);

    }

    @Test
    public void testEncodeCSharpMinValue(){

        BigDecimal value = new BigDecimal("-79228162514264337593543950335");

        byte[] actual = castDecimalTransform.toBytes(value);
        assertEquals( "FF000000000000000000000001", bytesToHex(actual));

    }

    @Test
    public void testEncodeCSharpMaxValue(){

        BigDecimal value = new BigDecimal("79228162514264337593543950335");

        byte[] actual = castDecimalTransform.toBytes(value);
        assertEquals( "00FFFFFFFFFFFFFFFFFFFFFFFF", bytesToHex(actual));

    }



    @Test
    public void castFieldsWithSchema() {

        // Configure a transform:
        Map<String, String> props = new HashMap<>();
        props.put(CastDecimal.ConfigNames.SCALE, "4");
        props.put(CastDecimal.ConfigNames.FIELD, "fld1");

        castDecimalTransform.configure(props);

        // Build a sample record schema:
        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("fld1", Schema.BYTES_SCHEMA);
        Schema sampleSchema = builder.build();

        // Now populate a Struct with test values:
        Struct recordValue = new Struct(sampleSchema);

        BigDecimal dec = new BigDecimal("12345678910.1234");
        recordValue.put("fld1", castDecimalTransform.toByteBuffer(dec));


        // Apply the transformation:
        SourceRecord transformed = castDecimalTransform.apply(new SourceRecord(null, null, "topic", 0,
                sampleSchema, recordValue));

        // Make assertions about the new value:
        assertEquals( "12345678910.1234", ((BigDecimal) ((Struct) transformed.value()).get("fld1")).toPlainString());


        // Make assertions about the new schema:
        Schema transformedSchema = ((Struct) transformed.value()).schema();
        assertEquals(Decimal.schema(4).type(), transformedSchema.field("fld1").schema().type());



        //assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("fld1").schema().type());
//        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("bigdecimal").schema().type());


        // Now we can assert the cast has been applied:

//        Date day = new Date(MILLIS_PER_DAY);
//        xformValue.configure(Collections.singletonMap(Cast.SPEC_CONFIG, "int8:int16,int16:int32,int32:int64,int64:boolean,float32:float64,float64:boolean,boolean:int8,string:int32,bigdecimal:string,date:string,optional:int32"));
//
//        // Include an optional fields and fields with defaults to validate their values are passed through properly
//        SchemaBuilder builder = SchemaBuilder.struct();
//        builder.field("int8", Schema.INT8_SCHEMA);
//        builder.field("int16", Schema.OPTIONAL_INT16_SCHEMA);
//        builder.field("int32", SchemaBuilder.int32().defaultValue(2).build());
//        builder.field("int64", Schema.INT64_SCHEMA);
//        builder.field("float32", Schema.FLOAT32_SCHEMA);
//        // Default value here ensures we correctly convert default values
//        builder.field("float64", SchemaBuilder.float64().defaultValue(-1.125).build());
//        builder.field("boolean", Schema.BOOLEAN_SCHEMA);
//        builder.field("string", Schema.STRING_SCHEMA);
//        builder.field("bigdecimal", Decimal.schema(new BigDecimal(42).scale()));
//        builder.field("date", Timestamp.SCHEMA);
//        builder.field("optional", Schema.OPTIONAL_FLOAT32_SCHEMA);
//        builder.field("timestamp", Timestamp.SCHEMA);
//        Schema supportedTypesSchema = builder.build();
//
//        Struct recordValue = new Struct(supportedTypesSchema);
//        recordValue.put("int8", (byte) 8);
//        recordValue.put("int16", (short) 16);
//        recordValue.put("int32", 32);
//        recordValue.put("int64", (long) 64);
//        recordValue.put("float32", 32.f);
//        recordValue.put("float64", -64.);
//        recordValue.put("boolean", true);
//        recordValue.put("bigdecimal", new BigDecimal(42));
//        recordValue.put("date", day);
//        recordValue.put("string", "42");
//        recordValue.put("timestamp", new Date(0));
//        // optional field intentionally omitted
//
//        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0,
//                supportedTypesSchema, recordValue));
//
//        assertEquals((short) 8, ((Struct) transformed.value()).get("int8"));
//        assertTrue(((Struct) transformed.value()).schema().field("int16").schema().isOptional());
//        assertEquals(16, ((Struct) transformed.value()).get("int16"));
//        assertEquals((long) 32, ((Struct) transformed.value()).get("int32"));
//        assertEquals(2L, ((Struct) transformed.value()).schema().field("int32").schema().defaultValue());
//        assertEquals(true, ((Struct) transformed.value()).get("int64"));
//        assertEquals(32., ((Struct) transformed.value()).get("float32"));
//        assertEquals(true, ((Struct) transformed.value()).get("float64"));
//        assertEquals(true, ((Struct) transformed.value()).schema().field("float64").schema().defaultValue());
//        assertEquals((byte) 1, ((Struct) transformed.value()).get("boolean"));
//        assertEquals(42, ((Struct) transformed.value()).get("string"));
//        assertEquals("42", ((Struct) transformed.value()).get("bigdecimal"));
//        assertEquals(Values.dateFormatFor(day).format(day), ((Struct) transformed.value()).get("date"));
//        assertEquals(new Date(0), ((Struct) transformed.value()).get("timestamp"));
//        assertNull(((Struct) transformed.value()).get("optional"));
//
//        Schema transformedSchema = ((Struct) transformed.value()).schema();
//        assertEquals(Schema.INT16_SCHEMA.type(), transformedSchema.field("int8").schema().type());
//        assertEquals(Schema.OPTIONAL_INT32_SCHEMA.type(), transformedSchema.field("int16").schema().type());
//        assertEquals(Schema.INT64_SCHEMA.type(), transformedSchema.field("int32").schema().type());
//        assertEquals(Schema.BOOLEAN_SCHEMA.type(), transformedSchema.field("int64").schema().type());
//        assertEquals(Schema.FLOAT64_SCHEMA.type(), transformedSchema.field("float32").schema().type());
//        assertEquals(Schema.BOOLEAN_SCHEMA.type(), transformedSchema.field("float64").schema().type());
//        assertEquals(Schema.INT8_SCHEMA.type(), transformedSchema.field("boolean").schema().type());
//        assertEquals(Schema.INT32_SCHEMA.type(), transformedSchema.field("string").schema().type());
//        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("bigdecimal").schema().type());
//        assertEquals(Schema.STRING_SCHEMA.type(), transformedSchema.field("date").schema().type());
//        assertEquals(Schema.OPTIONAL_INT32_SCHEMA.type(), transformedSchema.field("optional").schema().type());
//        // The following fields are not changed
//        assertEquals(Timestamp.SCHEMA.type(), transformedSchema.field("timestamp").schema().type());
    }

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public byte[] decodeHexString(String hexString) {
        if (hexString.length() % 2 == 1) {
            throw new IllegalArgumentException(
                    "Invalid hexadecimal String supplied.");
        }

        byte[] bytes = new byte[hexString.length() / 2];
        for (int i = 0; i < hexString.length(); i += 2) {
            bytes[i / 2] = hexToByte(hexString.substring(i, i + 2));
        }
        return bytes;
    }

    public byte hexToByte(String hexString) {
        int firstDigit = toDigit(hexString.charAt(0));
        int secondDigit = toDigit(hexString.charAt(1));
        return (byte) ((firstDigit << 4) + secondDigit);
    }

    private int toDigit(char hexChar) {
        int digit = Character.digit(hexChar, 16);
        if(digit == -1) {
            throw new IllegalArgumentException(
                    "Invalid Hexadecimal Character: "+ hexChar);
        }
        return digit;
    }

}
