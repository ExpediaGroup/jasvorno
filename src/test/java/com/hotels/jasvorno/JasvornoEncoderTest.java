/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.junit.Test;

import com.hotels.Schemas;

public class JasvornoEncoderTest {

  @Test
  public void union() throws Exception {
    Schema schema = Schemas.simpleUnion();
    Record avro = new GenericRecordBuilder(schema).set("id", 1L).set("str", "hello").build();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new JasvornoEncoder(schema, outputStream);
    DatumWriter<Record> datumWriter = new GenericDatumWriter<Record>(schema);
    datumWriter.write(avro, encoder);
    encoder.flush();
    assertThat(new String(outputStream.toByteArray()), is("{\"id\":1,\"str\":\"hello\"}"));
    outputStream.close();
  }

  @Test
  public void unionNull() throws Exception {
    Schema schema = Schemas.simpleUnion();
    Record avro = new GenericRecordBuilder(schema).set("id", 1L).set("str", null).build();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new JasvornoEncoder(schema, outputStream);
    DatumWriter<Record> datumWriter = new GenericDatumWriter<Record>(schema);
    datumWriter.write(avro, encoder);
    encoder.flush();
    assertThat(new String(outputStream.toByteArray()), is("{\"id\":1,\"str\":null}"));
    outputStream.close();
  }

  @Test
  public void bytes() throws Exception {
    Schema schema = Schemas.bytes();
    Record avro = new GenericRecordBuilder(schema).set("str", ByteBuffer.wrap(new byte[] { 0x0, 0x1, 0x2 })).build();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new JasvornoEncoder(schema, outputStream);
    DatumWriter<Record> datumWriter = new GenericDatumWriter<Record>(schema);
    datumWriter.write(avro, encoder);
    encoder.flush();
    assertThat(new String(outputStream.toByteArray()), is("{\"str\":\"\\u0000\\u0001\\u0002\"}"));
    outputStream.close();
  }

}
