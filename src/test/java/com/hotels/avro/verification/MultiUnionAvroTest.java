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
package com.hotels.avro.verification;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Before;
import org.junit.Test;

import com.hotels.Schemas;

/**
 * This uses AvroJson to decode the JSON, resolves unions strictly using the schema. Cannot handle unions values that
 * are not explicitly typed.
 * <p></p>
 * Multi union; null, string, long
 */
public class MultiUnionAvroTest {

  public Schema schema = Schemas.multiUnion();
  private DatumReader<Object> datumReader;

  @Before
  public void setupSchema() throws Exception {
    datumReader = new GenericDatumReader<Object>(schema);
  }

  @Test
  public void encodeNullValueExplicit() throws Exception {
    String json = "{ \"id\": 1, \"str\": null }";
    Decoder decoder = createDecoder(json);
    Record avro = (GenericData.Record) datumReader.read(null, decoder);
    assertThat(avro.toString(), is("{\"id\": 1, \"str\": null}"));
  }

  @Test(expected = AvroTypeException.class)
  public void encodeNullValueMissing() throws Exception {
    String json = "{ \"id\": 1 }";
    Decoder decoder = createDecoder(json);
    datumReader.read(null, decoder);
  }

  @Test(expected = AvroTypeException.class)
  public void encodeValue1ArbitraryJson() throws Exception {
    String json = "{ \"id\": 1, \"str\": \"hello\" }";
    Decoder decoder = createDecoder(json);
    datumReader.read(null, decoder);
  }

  @Test
  public void encodeValue1AvroCompatible() throws Exception {
    String json = "{ \"id\": 1, \"str\": { \"string\": \"hello\"} }";
    Decoder decoder = createDecoder(json);
    Record avro = (GenericData.Record) datumReader.read(null, decoder);
    assertThat(avro.toString(), is("{\"id\": 1, \"str\": \"hello\"}"));
  }

  @Test(expected = AvroTypeException.class)
  public void encodeValue2ArbitraryJson() throws Exception {
    String json = "{ \"id\": 1, \"str\": 2 }";
    Decoder decoder = createDecoder(json);
    datumReader.read(null, decoder);
  }

  @Test
  public void encodeValue2AvroCompatible() throws Exception {
    String json = "{ \"id\": 1, \"str\": { \"long\": 2} }";
    Decoder decoder = createDecoder(json);
    Record avro = (GenericData.Record) datumReader.read(null, decoder);
    assertThat(avro.toString(), is("{\"id\": 1, \"str\": 2}"));
  }

  private Decoder createDecoder(String json) throws Exception {
    InputStream input = new ByteArrayInputStream(json.getBytes());
    DataInputStream din = new DataInputStream(input);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
    return decoder;
  }

}
