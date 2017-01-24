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
 * This is oddly asymmetric
 */
public class BytesAvroTest {

  public Schema schema = Schemas.bytes();
  private DatumReader<Object> datumReader;

  @Before
  public void setupSchema() throws Exception {
    datumReader = new GenericDatumReader<Object>(schema);
  }

  /* Expected to pass, but no */
  @Test(expected = AvroTypeException.class)
  public void encodeValueAvroCompatible() throws Exception {
    String json = "{ \"str\": {\"bytes\": \"AAEC\"} }";
    Decoder decoder = createDecoder(json);
    datumReader.read(null, decoder);
  }

  /* Expected to fail, but no */
  @Test
  public void encodeValueArbitraryJson() throws Exception {
    String json = "{ \"str\": \"AAEC\" }";
    Decoder decoder = createDecoder(json);
    Record avro = (GenericData.Record) datumReader.read(null, decoder);
    assertThat(avro.toString(), is("{\"str\": {\"bytes\": \"AAEC\"}}"));
  }

  private Decoder createDecoder(String json) throws Exception {
    InputStream input = new ByteArrayInputStream(json.getBytes());
    DataInputStream din = new DataInputStream(input);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
    return decoder;
  }

}
