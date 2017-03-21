package com.hotels.avro.verification;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidatorBuilder;
import org.junit.Test;

/** This test simply checks our understanding of Avro. */
public class UndeclaredFieldsTest {

  /**
   * It is valid when using a canBeReadStrategy, for messages will arrive with fields not declared in the earlier
   * schema.
   */
  @Test
  public void verifyAvroBehaviour_SchemaValidator() throws SchemaValidationException {
    Schema newSchema = SchemaBuilder.record("a").fields().requiredString("x").requiredString("y").endRecord();
    Schema oldSchema = SchemaBuilder.record("a").fields().requiredString("x").endRecord();
    List<Schema> oldSchemas = Collections.singletonList(oldSchema);
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll().validate(newSchema, oldSchemas);
  }

  /**
   * It is valid when using a canBeReadStrategy, for messages will arrive with fields not declared in the earlier
   * schema.
   */
  @Test
  public void verifyAvroBehaviour_SchemaCompatibility() throws SchemaValidationException {
    Schema newSchema = SchemaBuilder.record("a").fields().requiredString("x").requiredString("y").endRecord();
    Schema oldSchema = SchemaBuilder.record("a").fields().requiredString("x").endRecord();
    SchemaCompatibilityType compatibilityType = SchemaCompatibility
        .checkReaderWriterCompatibility(oldSchema, newSchema)
        .getType();
    assertThat(compatibilityType, is(SchemaCompatibilityType.COMPATIBLE));
  }
  
}
