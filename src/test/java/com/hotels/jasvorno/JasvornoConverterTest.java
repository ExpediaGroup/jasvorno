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
package com.hotels.jasvorno;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.avro.verification.UndeclaredFieldsTest;
import com.hotels.jasvorno.JasvornoConverter.MatchType;
import com.hotels.jasvorno.JasvornoConverter.UndeclaredFieldBehaviour;
import com.hotels.jasvorno.JasvornoConverter.UnionResolution;

public class JasvornoConverterTest {

  public GenericData model = GenericData.get();
  public ObjectMapper mapper = new ObjectMapper();

  @Test
  public void happy() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .requiredString("name")
        .endRecord();
    String json = "{ \"id\": 1, \"name\": \"bob\" }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(model, datum, schema);
  }

  @Test(expected = JasvornoConverterException.class)
  public void unknownFields() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .requiredString("name")
        .endRecord();
    String json = "{ \"id\": 1, \"name\": \"bob\", \"unknown\": \"x\", \"other\": \"y\" }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(model, datum, schema);
  }

  /** See: {@link UndeclaredFieldsTest}. */
  public void unknownFieldsIgnored() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .requiredString("name")
        .endRecord();
    String json = "{ \"id\": 1, \"name\": \"bob\", \"unknown\": \"x\", \"other\": \"y\" }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(model, datum, schema, UndeclaredFieldBehaviour.IGNORE);
  }

  @Test
  public void happyOptional() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .optionalString("name")
        .endRecord();
    String json = "{ \"id\": 1 }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(model, datum, schema);
  }

  @Test(expected = JasvornoConverterException.class)
  public void unknownWithOptional() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .optionalString("name")
        .endRecord();
    String json = "{ \"id\": 1, \"unknown\": \"x\", \"other\": \"y\" }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(model, datum, schema);
  }

  @Test
  public void matchArrayElementEmptyArray() throws Exception {
    Schema schema = SchemaBuilder.array().items().intType();
    JsonNode datum = mapper.readTree("[]");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchArrayElement(datum,
        schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchArrayElementOfCorrectType() throws Exception {
    Schema schema = SchemaBuilder.array().items().intType();
    JsonNode datum = mapper.readTree("[1,2]");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchArrayElement(datum,
        schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchArrayElementOfIncorrectType() throws Exception {
    Schema schema = SchemaBuilder.array().items().intType();
    JsonNode datum = mapper.readTree("[\"1\",\"2\"]");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchArrayElement(datum,
        schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  @Test
  public void matchArrayElementOfAmbiguousType() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema b = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("e").endRecord();
    Schema schema = SchemaBuilder.array().items().type(SchemaBuilder.unionOf().type(a).and().type(b).endUnion());
    JsonNode datum = mapper.readTree("[{\"n\":\"1\"}]");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchArrayElement(datum,
        schema);
    assertThat(matchType, is(MatchType.AMBIGUOUS));
  }

  @Test
  public void matchMapValueEmptyMap() throws Exception {
    Schema schema = SchemaBuilder.map().values().intType();
    JsonNode datum = mapper.readTree("{}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfCorrectType() throws Exception {
    Schema schema = SchemaBuilder.map().values().intType();
    JsonNode datum = mapper.readTree("{\"1\":2}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfIncorrectType() throws Exception {
    Schema schema = SchemaBuilder.map().values().intType();
    JsonNode datum = mapper.readTree("{\"1\":\"2\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  @Test
  public void matchMapValueOfAmbiguousType() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema b = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("e").endRecord();
    Schema schema = SchemaBuilder.map().values().type(SchemaBuilder.unionOf().type(a).and().type(b).endUnion());
    JsonNode datum = mapper.readTree("{\"1\":{\"n\":\"1\"}}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.AMBIGUOUS));
  }

  @Test
  public void matchRecordFull() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .optionalDouble("d")
        .nullableString("s", "def")
        .endRecord();
    JsonNode datum = mapper.readTree("{\"i\":1,\"d\":2.5,\"s\":\"Hello\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchRecordAmbiguousViaNullable() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .optionalDouble("d")
        .nullableString("s", "def")
        .endRecord();
    JsonNode datum = mapper.readTree("{\"i\":1,\"s\":\"Hello\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.AMBIGUOUS));
  }

  @Test
  public void matchRecordAmbiguousViaDefault() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .optionalDouble("d")
        .nullableString("s", "def")
        .endRecord();
    JsonNode datum = mapper.readTree("{\"i\":1,\"d\":2.5}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.AMBIGUOUS));
  }

  @Test
  public void matchRecordFullAndFullField() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .name("r")
        .type(SchemaBuilder.array().items().intType())
        .noDefault()
        .endRecord();
    JsonNode datum = mapper.readTree("{\"i\":1,\"r\":[1]}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchRecordNoneMissingField() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .optionalDouble("d")
        .nullableString("s", "def")
        .endRecord();
    JsonNode datum = mapper.readTree("{\"d\":2.5,\"s\":\"Hello\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  @Test
  public void matchRecordNoneExtraJsonField() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .optionalDouble("d")
        .nullableString("s", "def")
        .endRecord();
    JsonNode datum = mapper.readTree("{\"i\":1,\"d\":2.5,\"s\":\"Hello\",\"extra\":0}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  /** See: {@link UndeclaredFieldsTest}. */
  @Test
  public void matchRecordFullExtraJsonField() throws Exception {
    Schema schema = SchemaBuilder
        .record("a")
        .fields()
        .requiredInt("i")
        .optionalDouble("d")
        .nullableString("s", "def")
        .endRecord();
    JsonNode datum = mapper.readTree("{\"i\":1,\"d\":2.5,\"s\":\"Hello\",\"extra\":0}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.IGNORE).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void unionOneFullMatchOneNoMatch() throws Exception {
    Schema schema = SchemaBuilder.unionOf().intType().and().stringType().endUnion();
    JsonNode datum = mapper.readTree("\"Hello\"");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(Schema.create(Schema.Type.STRING)));
  }

  @Test
  public void unionNoMatches() throws Exception {
    Schema schema = SchemaBuilder.unionOf().intType().and().doubleType().endUnion();
    JsonNode datum = mapper.readTree("\"Hello\"");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.NONE));
    assertThat(unionResolution.schema, is(nullValue()));
  }

  @Test
  public void unionOneFullMatchOneAmbiguousMatch() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").endRecord();
    Schema b = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(a).and().type(b).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(a));
  }

  @Test
  public void unionTwoFullMatches() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").endRecord();
    Schema b = SchemaBuilder.record("b").fields().requiredString("n").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(a).and().type(b).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema.getType(), is(Schema.Type.RECORD));
  }

  @Test
  public void unionOneFullMatchOneAmbiguousMatchRecordFavoursFull() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("o").endRecord();
    Schema b = SchemaBuilder.record("b").fields().requiredString("n").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(a).and().type(b).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(b));
  }

  @Test
  public void unionTwoRecordAmbiguities() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema b = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("e").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(a).and().type(b).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.AMBIGUOUS));
    assertThat(unionResolution.schema, is(a)); // the first ambiguous match
  }

  @Test
  public void unionOneAmbiguousMatchPromotedToFull() throws Exception {
    Schema a = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(a).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(a)); // expected
  }

}
