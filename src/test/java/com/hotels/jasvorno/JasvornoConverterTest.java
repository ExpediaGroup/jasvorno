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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
  @Test
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
  public void happyWithoutModel() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .requiredString("name")
        .endRecord();
    String json = "{ \"id\": 1, \"name\": \"bob\" }";
    JsonNode datum = mapper.readTree(json);
    JasvornoConverter.convertToAvro(datum, schema, UndeclaredFieldBehaviour.IGNORE);
  }

  @Test
  public void nullDatumValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().record("record").fields().requiredLong("id").endRecord();
    Object obj = JasvornoConverter.convertToAvro(null, schema, UndeclaredFieldBehaviour.IGNORE);
    assertNull(obj);
  }

  @Test
  public void nullDatumValueWithModel() throws Exception {
    Schema schema = SchemaBuilder
        .builder()
        .record("record")
        .fields()
        .requiredLong("id")
        .requiredString("name")
        .endRecord();
    Object obj = JasvornoConverter.convertToAvro(model, null, schema, UndeclaredFieldBehaviour.IGNORE);
    assertNull(obj);
  }

  @Test(expected = JasvornoConverterException.class)
  public void datumWithNullValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().record("record").fields().requiredString("name").endRecord();
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode dataTable = mapper.createObjectNode();
    JsonNode datum = dataTable.putNull("name");
    JasvornoConverter.convertToAvro(model, datum, schema, UndeclaredFieldBehaviour.IGNORE);
  }

  @Test
  public void datumWithNoValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder
        .record("record")
        .fields()
        .name("name")
        .type()
        .stringType()
        .stringDefault("x")
        .endRecord();
    JsonNode datum = mapper.readTree("{}");
    Object obj = JasvornoConverter.convertToAvro(model, datum, schema, UndeclaredFieldBehaviour.IGNORE);
    assertTrue(obj instanceof IndexedRecord);
    IndexedRecord record = (IndexedRecord) obj;
    assertThat(record.get(0).toString(), is("x"));
  }

  @Test
  public void schemaOfTypeMap() throws Exception {
    Schema schema = SchemaBuilder.map().values().stringType();
    String json = "{\"name\": \"bob\", \"surname\": \"blue\" }";
    JsonNode datum = mapper.readTree(json);
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> actualMap = (Map<String, Object>) obj;
    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put("name", "bob");
    expectedMap.put("surname", "blue");
    assertTrue(actualMap.equals(expectedMap));
  }

  @Test
  public void schemaOfTypeArray() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.array().items().intType();
    JsonNode datum = mapper.readTree("[1,2]");
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof List);
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) obj;
    assertTrue(list.equals(Arrays.asList(1, 2)));
  }

  @Test
  public void schemaOfTypeBoolean() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().booleanType();
    JsonNode datum = mapper.readTree("true");
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof Boolean);
    assertThat(obj, is(true));
  }

  @Test
  public void schemaOfTypeFloat() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().floatType();
    JsonNode datum = mapper.readTree("2.4");
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof Float);
    assertThat(obj, is(2.4F));
  }

  @Test
  public void schemaOfTypeDouble() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().doubleType();
    JsonNode datum = mapper.readTree("2.4");
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof Double);
    assertThat(obj, is(2.4));
  }

  @Test
  public void schemaOfTypeEnum() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().enumeration("Suits").namespace("org.cards").doc("card suit names").symbols(
        "HEART", "SPADE", "DIAMOND", "CLUB");
    JsonNode datum = mapper.readTree("\"HEART\"");
    Object actualObj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(actualObj instanceof EnumSymbol);
    assertThat(actualObj, is(new EnumSymbol(schema, "HEART")));
  }

  @Test(expected = JasvornoConverterException.class)
  public void schemaOfTypeFixedTooSmall() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().fixed("com.foo.IPv4").size(4);
    JsonNode datum = mapper.readTree("\"zzz\"");
    JasvornoConverter.convertToAvro(datum, schema);
  }

  @Test
  public void schemaOfTypeFixed() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().fixed("com.foo.IPv4").size(4);
    JsonNode datum = mapper.readTree("\"zzzz\"");
    Object actualObj = JasvornoConverter.convertToAvro(datum, schema);
    byte[] bytes = ("zzzz").getBytes();
    GenericFixed expected = new Fixed(schema, bytes);
    assertTrue(actualObj instanceof GenericFixed);
    assertThat(actualObj, is(expected));
  }

  @Test(expected = JasvornoConverterException.class)
  public void schemaOfTypeFixedTooLarge() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().fixed("com.foo.IPv4").size(4);
    JsonNode datum = mapper.readTree("\"192.148.1.1\"");
    JasvornoConverter.convertToAvro(datum, schema);
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
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schemaB = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("e").endRecord();
    Schema schema = SchemaBuilder
        .array()
        .items()
        .type(SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion());
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
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schemaB = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("e").endRecord();
    Schema schema = SchemaBuilder
        .map()
        .values()
        .type(SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion());
    JsonNode datum = mapper.readTree("{\"1\":{\"n\":\"1\"}}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.AMBIGUOUS));
  }

  @Test
  public void matchMapValueOfBooleanType() throws Exception {
    Schema schema = SchemaBuilder.map().values().booleanType();
    JsonNode datum = mapper.readTree("{\"1\":true}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\":2.4}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithMaxDoubleValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\":" + (double) Float.MAX_VALUE + "}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithMinDoubleValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\":" + (double) -Float.MAX_VALUE + "}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithLong() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\":" + 922337203685477589L + "}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithMaxLongValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\":" + ((long) Float.MAX_VALUE) + "}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithMinLongValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\":" + ((long) -Float.MAX_VALUE) + "}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithFloatValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode dataTable = mapper.createObjectNode();
    JsonNode datum = dataTable.put("field1", Float.MAX_VALUE);
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfFloatTypeWithIntValue() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().floatType();
    JsonNode datum = mapper.readTree("{\"1\": 2}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfDoubleType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().doubleType();
    JsonNode datum = mapper.readTree("{\"1\": 2.4}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfDoubleWrongType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().doubleType();
    JsonNode datum = mapper.readTree("{\"1\": \"bob\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  @Test
  public void matchMapValueofLongType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().longType();
    JsonNode datum = mapper.readTree("{\"1\": 2}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueOfLongTypeWrongType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().longType();
    JsonNode datum = mapper.readTree("{\"1\": \"bob\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  @Test
  public void matchMapValueofFixedType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().fixed("com.foo.IPv4").size(4);
    JsonNode datum = mapper.readTree("{\"1\": \"zzzz\"}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
  }

  @Test
  public void matchMapValueofFixedTypeWrongType() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.map().values().fixed("com.foo.IPv4").size(4);
    JsonNode datum = mapper.readTree("{\"1\": 2}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
  }

  @Test
  public void matchMapValueofNullDatum() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.builder().map().values().nullType();
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode dataTable = mapper.createObjectNode();
    JsonNode datum = dataTable.putNull("field1");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).matchMapValue(datum, schema);
    assertThat(matchType, is(MatchType.FULL));
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
  public void matchRecordNoMatch() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder.record("record").fields().requiredString("name").endRecord();
    JsonNode datum = mapper.readTree("{\"name\": 1}");
    MatchType matchType = new JasvornoConverter(model, UndeclaredFieldBehaviour.IGNORE).matchRecord(datum, schema);
    assertThat(matchType, is(MatchType.NONE));
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
  public void unionNoMatchesWithPrimitives() throws Exception {
    Schema schema = SchemaBuilder.unionOf().intType().and().doubleType().endUnion();
    JsonNode datum = mapper.readTree("true");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.NONE));
    assertThat(unionResolution.schema, is(nullValue()));
  }

  @Test
  public void unionOneFullMatchOneAmbiguousMatch() throws Exception {
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").endRecord();
    Schema schemaB = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionTwoFullMatches() throws Exception {
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").endRecord();
    Schema schemaB = SchemaBuilder.record("b").fields().requiredString("n").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema.getType(), is(Schema.Type.RECORD));
  }

  @Test
  public void unionOneFullMatchOneAmbiguousMatchRecordFavoursFull() throws Exception {
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("o").endRecord();
    Schema schemaB = SchemaBuilder.record("b").fields().requiredString("n").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaB));
  }

  @Test
  public void unionTwoRecordAmbiguities() throws Exception {
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schemaB = SchemaBuilder.record("b").fields().requiredString("n").optionalDouble("e").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.AMBIGUOUS));
    assertThat(unionResolution.schema, is(schemaA)); // the first ambiguous match
  }

  @Test
  public void unionOneAmbiguousMatchPromotedToFull() throws Exception {
    Schema schemaA = SchemaBuilder.record("a").fields().requiredString("n").optionalDouble("d").endRecord();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).endUnion();
    JsonNode datum = mapper.readTree("{\"n\":\"y\"}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA)); // expected
  }

  @Test
  public void unionDoubleInFloatRangeMax() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().floatType();
    Schema schemaB = SchemaBuilder.builder().intType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = new DoubleNode(Float.MAX_VALUE);
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionDoubleInFloatRangeMin() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().floatType();
    Schema schemaB = SchemaBuilder.builder().intType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = new DoubleNode(-Float.MAX_VALUE);
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionLongInFloatRangeMax() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().floatType();
    Schema schemaB = SchemaBuilder.builder().intType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = new LongNode((long) Float.MAX_VALUE);
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionLongInFloatRangeMin() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().floatType();
    Schema schemaB = SchemaBuilder.builder().intType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = new LongNode((long) -Float.MAX_VALUE);
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionLongInsideFloatRange() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().floatType();
    Schema schemaB = SchemaBuilder.builder().intType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).and().type(schemaB).endUnion();
    JsonNode datum = new LongNode((long) (0.5 * Float.MAX_VALUE));
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionBoolean() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().booleanType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).endUnion();
    JsonNode datum = mapper.readTree("true");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void unionMap() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.map().values().intType();
    Schema schema = SchemaBuilder.unionOf().type(schemaA).endUnion();
    JsonNode datum = mapper.readTree("{\"n\": 2}");
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA)); // expected
  }

  @Test
  public void unionEnum() throws JsonProcessingException, IOException {
    Schema schemaA = SchemaBuilder.builder().enumeration("Suits").namespace("org.cards").doc("card suit names").symbols(
        "HEART", "SPADE", "DIAMOND", "CLUB");
    JsonNode datum = mapper.readTree("\"HEART\"");
    Schema schema = SchemaBuilder.unionOf().type(schemaA).endUnion();
    UnionResolution unionResolution = new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH)
        .resolveUnion(datum, schema.getTypes());
    assertThat(unionResolution.matchType, is(MatchType.FULL));
    assertThat(unionResolution.schema, is(schemaA));
  }

  @Test
  public void nullOkWithFieldUnionOfNull() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder
        .record("record")
        .fields()
        .name("x")
        .type()
        .nullable()
        .booleanType()
        .booleanDefault(false)
        .endRecord();
    JsonNode datum = mapper.readTree("{}");
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof IndexedRecord);
    IndexedRecord record = (IndexedRecord) obj;
    assertNull(record.get(0));
  }

  @Test
  public void nullOkWithFieldUnionNotNull() throws JsonProcessingException, IOException {
    Schema schema = SchemaBuilder
        .record("record")
        .fields()
        .name("x")
        .type()
        .unionOf()
        .stringType()
        .and()
        .intType()
        .endUnion()
        .stringDefault("x")
        .endRecord();
    JsonNode datum = mapper.readTree("{}");
    Object obj = JasvornoConverter.convertToAvro(datum, schema);
    assertTrue(obj instanceof IndexedRecord);
    IndexedRecord record = (IndexedRecord) obj;
    assertThat(record.get(0).toString(), is("x"));
  }
}
