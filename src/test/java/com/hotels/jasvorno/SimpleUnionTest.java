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
package com.hotels.jasvorno;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.Schemas;

/**
 * This uses Kite to decode the JSON, resolves unions by making inferences from both the data and the schema. Cannot
 * handle AvroJson encoded unions for a given schema.
 * <p></p>
 * Simple null union: string/null
 */
public class SimpleUnionTest {

  public Schema schema = Schemas.simpleUnion();
  public GenericData model = GenericData.get();
  public ObjectMapper mapper = new ObjectMapper();

  @Test
  public void encodeNullValueExplicit() throws Exception {
    String json = "{ \"id\": 1, \"str\": null }";
    JsonNode datum = mapper.readTree(json);
    Record avro = (GenericData.Record) JasvornoConverter.convertToAvro(model, datum, schema);
    assertThat(avro.toString(), is("{\"id\": 1, \"str\": null}"));
  }

  @Test
  public void encodeNullValueMissing() throws Exception {
    String json = "{ \"id\": 1 }";
    JsonNode datum = mapper.readTree(json);
    Record avro = (GenericData.Record) JasvornoConverter.convertToAvro(model, datum, schema);
    assertThat(avro.toString(), is("{\"id\": 1, \"str\": null}"));
  }

  @Test
  public void encodeValueArbitraryJson() throws Exception {
    String json = "{ \"id\": 1, \"str\": \"hello\" }";
    JsonNode datum = mapper.readTree(json);
    Record avro = (GenericData.Record) JasvornoConverter.convertToAvro(model, datum, schema);
    assertThat(avro.toString(), is("{\"id\": 1, \"str\": \"hello\"}"));
  }

  @Test(expected = JasvornoConverterException.class)
  public void encodeValueAvroCompatible() throws Exception {
    String json = "{ \"id\": 1, \"str\": { \"string\": \"hello\"} }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(model, datum, schema);
  }

}
