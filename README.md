# JASVORNO

## Start using
You can obtain Jasvorno from Maven Central : 

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/jasvorno/badge.svg?subject=com.hotels:jasvorno)](https://maven-badges.herokuapp.com/maven-central/com.hotels/jasvorno) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/jasvorno.svg)

## Overview
A library for serializing/deserializing arbitrary JSON to the Avro format using a `Schema`. Although Avro already has some [inbuilt JSON support](https://avro.apache.org/docs/current/spec.html#json_encoding), it has a number of limitations that cannot be addressed in a backwards compatible way. Jasvorno offers supplementary classes that allow *any* JSON document structure to be used with Avro with robust checks for conformity to a schema. See the 'Avro limitations' section for more information.

## Usage
### JSON to Avro
    JsonNode datum = new ObjectMapper().readTree(jsonString);
    Object avro = JasvornoConverter.convertToAvro(GenericData.get(), datum, schema);
    
### Avro to JSON
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = new JasvornoEncoder(schema, outputStream);
    new GenericDatumWriter<Record>(schema).write(avro, encoder);
    encoder.flush();
    String json = new String(outputStream.toByteArray())

## Comparison
Given the following schema, compare compliant JSON document structures used by the standard Avro JSON encoding and Jasvorno's encoding. Note that the Jasvorno documents are not polluted with union type indexes.

    {"type": "record", "name": "myrecord",
     "fields" : [
       {"name": "id",  "type": "long"},
       {"name": "val", "type": ["string", "long", "null"]}
     ]
    }

### Avro encoding examples
    {"id": 1, "val": {"string": "hello"}}
    {"id": 1, "val": {"long": 2}}
    {"id": 1, "val": null}

### Jasvorno encoding examples
    {"id": 1, "val": "hello"}
    {"id": 1, "val": 2}
    {"id": 1, "val": null}
    {"id": 1}
    
## Avro limitations
Although Avro already has inbuilt JSON coders: `JsonDecoder`, `JsonEncoder`. These require Avro specific document structures, primarily for handling `union` types. Avro also offers support for free form JSON document structures with the `org.apache.avro.data.Json` type, but this only checks for general JSON compliance (i.e. that it is a valid JSON document) rather than the structure of the node tree that the document declares. These limitations prevent the direct use of arbitrary JSON documents structures in a strict, schema enforced manner.

This is a problem because it either requires users to transform their JSON into Avro compatible forms, or for Avro specific implementation details to leak out into user's JSON models. Jasvorno solves this.

## Implementation details
Jasvorno does not currently follow Avro's encoder/decoder symmetry. Instead we use the `JasvornoConverter` in place of a `Decoder` implementation. This is because in the absence of the `union` indexes present in Avro's own JSON document structures, we must preemptively explore the document tree to determine the best `Schema` match. Therefore it is easier to read the entire JSON document up front and then check for `Schema` compliance against this, converting to Avro along the way. A potential problem with is that this approach might be expensive for schemas containing many, deep union types. Additionally there are type constructs that are impossible to disambiguate by referencing JSON node values alone; concretely any union containing `bytes` and `string`. In this event we favour the `string` type but we also provide the `com.hotels.jasvorno.schema.SchemaValidator` should you wish to defensively detect these ambiguous constructs in your schemas.      

## Schema compatibility
Although Jasvorno does not directly deal with schema evolution and compatibility, these concepts are common in systems that use Avro. This is of particular concern when encountering fields that are present in a JSON document, but not declared in the schema. Under some schema compatibility modes such fields are erroneous, yet with others they are expected. To model these different situations appropriately Jasvorno allows you to specify a `UndeclaredFieldBehaviour` when constructing a `JasvornoConverter`.

## Prior art
Jasvorno is based on the [`JsonUtil`](https://github.com/kite-sdk/kite/blob/master/kite-data/kite-data-core/src/main/java/org/kitesdk/data/spi/JsonUtil.java) class from the [Kite project](http://kitesdk.org/docs/current/).

## Author
Created by [Elliot West](https://github.com/teabot), with thanks to [Adrian Woodhead](https://github.com/massdosage), [Dave Maughan](https://github.com/nahguam), and [James Grant](https://github.com/Noddy76).

## Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2017 Expedia Inc.
