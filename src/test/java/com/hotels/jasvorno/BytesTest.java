/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.util.Base64;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.Schemas;

public class BytesTest {

  public Schema schema = Schemas.bytes();
  public GenericData model = GenericData.get();
  public ObjectMapper mapper = new ObjectMapper();

  @Test
  public void encodeBytesFromJsonBase64EncodedText() {
    JsonNode datum = mapper.createObjectNode()
        .put("str", Base64.getEncoder().encodeToString("abc".getBytes()));
    GenericRecord avro = (GenericRecord) JasvornoConverter.convertToAvro(model, datum, schema);
    assertThat(new String(((ByteBuffer) avro.get("str")).array()), is("abc"));
  }

  @Test
  public void encodeBytesFromJsonBinary() {
    JsonNode datum = mapper.createObjectNode()
        .put("str", "abc".getBytes());
    GenericRecord avro = (GenericRecord) JasvornoConverter.convertToAvro(model, datum, schema);
    assertThat(new String(((ByteBuffer) avro.get("str")).array()), is("abc"));
  }

}
