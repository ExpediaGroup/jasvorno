/**
 * Copyright (C) 2015-2017 Expedia Inc.
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
package com.hotels.jasvorno.schema;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import com.hotels.Schemas;

public class SchemaValidatorTest {

  @Test
  public void noBytes() throws Exception {
    Schema schema = Schemas.multiUnion();
    assertThat(SchemaValidator.isValid(schema), is(true));
  }

  @Test
  public void bytesInRecord() throws Exception {
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(Schema.create(Type.BYTES))
        .noDefault()
        .endRecord();
    assertThat(SchemaValidator.isValid(schema), is(true));
  }

  @Test
  public void unionOfBytesAndString() throws Exception {
    Schema schema = SchemaBuilder.builder().unionOf().nullType().and().stringType().and().bytesType().endUnion();
    assertThat(SchemaValidator.isValid(schema), is(false));
  }

  @Test
  public void unionOfBytesAndStringInRecord() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .type(SchemaBuilder.builder().unionOf().stringType().and().bytesType().endUnion())
        .noDefault()
        .endRecord();
    assertThat(SchemaValidator.isValid(schema), is(false));
  }

  @Test
  public void unionOfBytesInRecord() throws Exception {
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion())
        .noDefault()
        .endRecord();
    assertThat(SchemaValidator.isValid(schema), is(true));
  }

  @Test
  public void unionOfStringInRecord() throws Exception {
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.builder().unionOf().nullType().and().stringType().endUnion())
        .noDefault()
        .endRecord();

    assertThat(SchemaValidator.isValid(schema), is(true));
  }

  @Test
  public void mapBytesInRecord() throws Exception {
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.map().values(SchemaBuilder.builder().bytesType()))
        .noDefault()
        .endRecord();
    assertThat(SchemaValidator.isValid(schema), is(true));
  }

  @Test
  public void arrayBytesInRecord() throws Exception {
    Schema schema = SchemaBuilder
        .record("X")
        .fields()
        .name("a")
        .type(SchemaBuilder.array().items(SchemaBuilder.builder().bytesType()))
        .noDefault()
        .endRecord();
    assertThat(SchemaValidator.isValid(schema), is(true));
  }

  @Test(expected = SchemaValidationException.class)
  public void unionOfBytesAndStringInRecordValidate() throws Exception {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .type(SchemaBuilder.builder().unionOf().stringType().and().bytesType().endUnion())
        .noDefault()
        .endRecord();
    SchemaValidator.validate(schema);
  }

  @Test
  public void unionOfBytesInRecordValidate() {
    Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .type(SchemaBuilder.builder().unionOf().nullType().and().bytesType().endUnion())
        .noDefault()
        .endRecord();
    try {
      SchemaValidator.validate(schema);
    } catch (SchemaValidationException e) {
      fail("Should not throw an exception");
    }
  }
}
