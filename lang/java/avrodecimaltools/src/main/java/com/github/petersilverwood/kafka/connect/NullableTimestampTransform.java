package com.github.petersilverwood.kafka.connect;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class NullableTimestampTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(NullableTimestampTransform.class);

    protected interface ConfigNames {
        String FIELD = "field";

    }

    private static final String PURPOSE = "cast unix ts to timestamp field";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(NullableTimestampTransform.ConfigNames.FIELD,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    (name, valueObject) -> {
                        if(valueObject == null || valueObject.equals("")){
                            throw new ConfigException("Must specify a field to cast.");
                        };
                    },
                    ConfigDef.Importance.HIGH,
                    "The field containing the unix ts");

    private String field;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        field = config.getString(NullableTimestampTransform.ConfigNames.FIELD);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }


    @Override
    public void close() {

    }


    @Override
    public R apply(R record) {

        // Check that the SMT is applied to a field in the record value only.
        if (null == record.valueSchema() || Schema.Type.STRUCT != record.valueSchema().type()) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not a struct.");
            return record;
        }

        // Build new schema
        Schema updatedSchema = getOrBuildSchema(record.valueSchema());


        // The record value must contain a Struct type, which we can clone to a new Struct with the updated schema.
        Struct value = requireStruct(record.value(), PURPOSE);

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {

            final Object origFieldValue = value.get(field);
            if(field.name().equals(this.field)){

                Object newFieldValue;
                // Do cast:
                if(origFieldValue == null){
                    newFieldValue = null;
                }else{
                    newFieldValue = new java.util.Date((long)origFieldValue);
                }

                updatedValue.put(updatedSchema.field(field.name()), newFieldValue);
                log.trace("Cast field '{}' from '{}' to '{}'", field.name(), origFieldValue, newFieldValue);
            }else{
                updatedValue.put(updatedSchema.field(field.name()), origFieldValue);
            }

        }

        // Return a new record:
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());

    }




    /*
    * Build the target schema
     */
    private Schema getOrBuildSchema(Schema valueSchema) {

        // Lookup from cache:
        Schema updatedSchema = schemaUpdateCache.get(valueSchema);
        if (updatedSchema != null)
            return updatedSchema;

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());

        for (Field field : valueSchema.fields()) {
            if (this.field.equals(field.name())) {

                builder.field(field.name(), Timestamp.builder().optional().schema());

            } else {
                builder.field(field.name(), field.schema());
            }
        }

        updatedSchema = builder.build();
        schemaUpdateCache.put(valueSchema, updatedSchema);
        return updatedSchema;


    }


}
