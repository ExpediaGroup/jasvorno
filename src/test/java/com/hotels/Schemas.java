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
package com.hotels;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public final class Schemas {

  private Schemas() {
  }

  public static Schema bytes() {
    return SchemaBuilder.record("mine").fields().requiredBytes("str").endRecord();
  }

  public static Schema simpleUnion() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder.unionOf().stringType().and().nullType().endUnion())
        .noDefault()
        .endRecord();
  }

  public static Schema ambiguousUnion() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder.unionOf().nullType().and().stringType().and().bytesType().endUnion())
        .noDefault()
        .endRecord();
  }

  public static Schema complexUnion() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder
            .unionOf()
            .stringType()
            .and()
            .longType()
            .and()
            .type(SchemaBuilder.record("inner").fields().requiredLong("a").requiredLong("b").endRecord())
            .and()
            .nullType()
            .endUnion())
        .noDefault()
        .endRecord();
  }

  public static Schema multiUnion() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder.unionOf().longType().and().stringType().and().nullType().endUnion())
        .noDefault()
        .endRecord();
  }

  public static Schema typeExpansionUnion() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder.unionOf().intType().and().longType().and().nullType().endUnion())
        .noDefault()
        .endRecord();
  }

  public static Schema nestedOk() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder
            .unionOf()
            .stringType()
            .and()
            .longType()
            .and()
            .type(SchemaBuilder
                .record("inner")
                .fields()
                .name("a")
                .type(SchemaBuilder.array().items().stringType())
                .noDefault()
                .name("b")
                .type(SchemaBuilder.map().values().longType())
                .noDefault()
                .endRecord())
            .and()
            .nullType()
            .endUnion())
        .noDefault()
        .endRecord();
  }

  public static Schema nestedFail() {
    return SchemaBuilder
        .record("mine")
        .fields()
        .requiredLong("id")
        .name("str")
        .type(SchemaBuilder
            .unionOf()
            .stringType()
            .and()
            .longType()
            .and()
            .type(SchemaBuilder
                .record("inner")
                .fields()
                .name("a")
                .type(SchemaBuilder.array().items().stringType())
                .noDefault()
                .name("b")
                .type(SchemaBuilder.map().values().bytesType())
                .noDefault()
                .endRecord())
            .and()
            .nullType()
            .endUnion())
        .noDefault()
        .endRecord();
  }

}
