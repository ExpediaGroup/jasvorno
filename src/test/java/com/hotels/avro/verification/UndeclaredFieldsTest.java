/**
 * Copyright (C) 2016-2017 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
   * It is valid when using a canBeReadStrategy for messages to arrive with fields not declared in the earlier
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
   * It is valid when using a canBeReadStrategy for messages to arrive with fields not declared in the earlier
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
