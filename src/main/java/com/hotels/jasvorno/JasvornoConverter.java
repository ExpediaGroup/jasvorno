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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Taken from Kite's {@code JsonUtil}. It is worth mentioning why this is not readily amenable to being implemented as a
 * {@link Decoder}. The support of {@code unions} without a corresponding type hint in the JSON document requires a
 * look-ahead into the document so that we can guess the type from the value and determine the appropriate union branch.
 * However, the {@link Decoder} model is very much driven from the {@link Schema}, not the data and thus it is not
 * possible to read ahead without first knowing the type one wishes to read (chicken/egg). While it might be possible to
 * implement some stack based {@link Decoder} that can rewind and try different branches as needed, I believe this
 * manner of operation would result in code that is far more confusing and difficult to debug and maintain. Additionally
 * I'm not certain that the Avro code calling the {@link Decoder} {@code readXXX()} methods can be coaxed into reading
 * one type, discarding it, then reading another. Ultimately all the magic would have to happen inside
 * {@link Decoder#readIndex()} and the code calling said method would need to be unaware of all of the magic happening
 * inside.
 */
public class JasvornoConverter {

  private static final Logger log = LoggerFactory.getLogger(JasvornoConverter.class);

  enum MatchType {
    /** The JSON document node aligns with the schema perfectly. */
    FULL,

    /** The JSON document node is incompatible with the schema. */
    NONE,

    /**
     * The JSON document node matches the schema, but given this instance, we don't have enough information to say
     * whether this is a perfect match. We make this distinction so that we can favour better matches if we find them,
     * but fall through to ambiguous matches if we don't. Ambiguous matches usually result from implicit null values,
     * and implicit default values in record types, In practice parsing against an 'AMBIGUOUS' matching schema should
     * still work.
     */
    AMBIGUOUS;
  }

  public enum UndeclaredFieldBehaviour {
    /** If there are fields in the JSON document that are not declared in the schema, ignore them. */
    IGNORE,
    /** If there are fields in the JSON document that are not declared in the schema then do not match the schema. */
    NO_MATCH;
  }

  @VisibleForTesting
  static class UnionResolution {
    static final UnionResolution NONE = new UnionResolution(null, MatchType.NONE);

    final Schema schema;
    final MatchType matchType;

    UnionResolution(Schema schema, MatchType matchType) {
      this.schema = schema;
      this.matchType = matchType;
    }
  }

  public static Object convertToAvro(JsonNode datum, Schema schema) {
    return new JasvornoConverter(GenericData.get(), UndeclaredFieldBehaviour.NO_MATCH).internalConvertToAvro(datum,
        schema);
  }

  public static Object convertToAvro(JsonNode datum, Schema schema, UndeclaredFieldBehaviour undeclaredFieldBehaviour) {
    return new JasvornoConverter(GenericData.get(), undeclaredFieldBehaviour).internalConvertToAvro(datum, schema);
  }

  public static Object convertToAvro(GenericData model, JsonNode datum, Schema schema) {
    return new JasvornoConverter(model, UndeclaredFieldBehaviour.NO_MATCH).internalConvertToAvro(datum, schema);
  }

  public static Object convertToAvro(
      GenericData model,
      JsonNode datum,
      Schema schema,
      UndeclaredFieldBehaviour undeclaredFieldBehaviour) {
    return new JasvornoConverter(model, undeclaredFieldBehaviour).internalConvertToAvro(datum, schema);
  }

  private final UndeclaredFieldBehaviour undeclaredFieldBehaviour;
  private final GenericData model;

  @VisibleForTesting
  JasvornoConverter(GenericData model, UndeclaredFieldBehaviour undeclaredFieldBehaviour) {
    this.model = model;
    this.undeclaredFieldBehaviour = undeclaredFieldBehaviour;
  }

  private Object internalConvertToAvro(JsonNode datum, Schema schema) {
    log.debug("Looking at type '{}', with name '{}'", schema.getType(), schema.getName());

    if (datum == null) {
      return null;
    }
    switch (schema.getType()) {
    case RECORD:
      JasvornoConverterException.check(datum.isObject(), "Cannot convert non-object to record: %s", datum);
      Object record = model.newRecord(null, schema);
      Set<String> jsonFields = new HashSet<>();
      Iterators.addAll(jsonFields, datum.fieldNames());
      for (Schema.Field field : schema.getFields()) {
        log.debug("Converting field '{}.{}'", schema.getName(), field.name());
        model.setField(record, field.name(), field.pos(), convertField(datum.get(field.name()), field));
        jsonFields.remove(field.name());
      }
      if (UndeclaredFieldBehaviour.NO_MATCH == undeclaredFieldBehaviour && !jsonFields.isEmpty()) {
        throw new JasvornoConverterException("JSON object contains fields not declared in the schema ["
            + schema.getNamespace()
            + "."
            + schema.getName()
            + "]: "
            + jsonFields);
      }
      return record;

    case MAP:
      JasvornoConverterException.check(datum.isObject(), "Cannot convert non-object to map: %s", datum);
      Map<String, Object> map = Maps.newLinkedHashMap();
      Iterator<Map.Entry<String, JsonNode>> iter = datum.fields();
      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> entry = iter.next();
        map.put(entry.getKey(), convertToAvro(model, entry.getValue(), schema.getValueType()));
      }
      return map;

    case ARRAY:
      JasvornoConverterException.check(datum.isArray(), "Cannot convert to array: %s", datum);
      List<Object> list = Lists.newArrayListWithExpectedSize(datum.size());
      for (JsonNode element : datum) {
        list.add(convertToAvro(model, element, schema.getElementType()));
      }
      return list;

    case UNION:
      UnionResolution unionResolution = resolveUnion(datum, schema.getTypes());
      if (unionResolution.matchType == MatchType.NONE) {
        throw new JasvornoConverterException(String.format("Cannot resolve union: %s %s not in %s",
            datum.getClass().getSimpleName(), datum, schema.getTypes()));
      }
      return convertToAvro(model, datum, unionResolution.schema);

    case BOOLEAN:
      JasvornoConverterException.check(datum.isBoolean(), "Cannot convert to boolean: %s", datum);
      return datum.booleanValue();

    case FLOAT:
      JasvornoConverterException
          .check((datum.isDouble() && datum.doubleValue() >= -Float.MAX_VALUE && datum.doubleValue() <= Float.MAX_VALUE)
              || (datum.isLong()
                  && datum.longValue() >= (long) -Float.MAX_VALUE
                  && datum.longValue() <= (long) Float.MAX_VALUE)
              || datum.isFloat()
              || datum.isInt(), "Cannot convert to float: %s", datum);
      return datum.floatValue();

    case DOUBLE:
      JasvornoConverterException.check(datum.isDouble() || datum.isFloat() || datum.isLong() || datum.isInt(),
          "Cannot convert to double: %s", datum);
      return datum.doubleValue();

    case INT:
      JasvornoConverterException.check(datum.isInt(), "Cannot convert to int: %s", datum);
      return datum.intValue();

    case LONG:
      JasvornoConverterException.check(datum.isLong() || datum.isInt(), "Cannot convert to long: %s", datum);
      return datum.longValue();

    case STRING:
      JasvornoConverterException.check(datum.isTextual(), "Cannot convert to string: %s", datum);
      return datum.textValue();

    case ENUM:
      JasvornoConverterException.check(datum.isTextual(), "Cannot convert to string: %s", datum);
      return model.createEnum(datum.textValue(), schema);

    case BYTES:
      JasvornoConverterException.check(datum.isTextual(), "Cannot convert to binary: %s", datum);
      try {
        // TODO: should this be ISO_8859_1?
        return ByteBuffer.wrap(datum.textValue().getBytes(Charsets.UTF_8));
      } catch (IllegalArgumentException e) {
        throw new JasvornoConverterException("Failed to read JSON binary, not a unicode escaped string", e);
      }

    case FIXED:
      JasvornoConverterException.check(datum.isTextual(), "Cannot convert to fixed: %s", datum);
      // TODO: should this be ISO_8859_1?
      byte[] bytes = datum.textValue().getBytes(Charsets.UTF_8);
      JasvornoConverterException.check(bytes.length == schema.getFixedSize(),
          "Byte length does not match schema size: %s bytes for %s", bytes.length, schema);
      return model.createFixed(null, bytes, schema);

    case NULL:
      return null;

    default:
      // don't use DatasetRecordException because this is a Schema problem
      throw new IllegalArgumentException("Unknown schema type: " + schema);
    }
  }

  private Object convertField(JsonNode datum, Schema.Field field) {
    try {
      Object value = convertToAvro(datum, field.schema());
      if (value != null || nullOk(field.schema())) {
        return value;
      } else {
        return model.getDefaultValue(field);
      }
    } catch (JasvornoConverterException e) {
      // add the field name to the error message
      throw new JasvornoConverterException(String.format("Cannot convert field %s", field.name()), e);
    } catch (AvroRuntimeException e) {
      throw new JasvornoConverterException(String.format("Field '%s': cannot make '%s' value: '%s'", field.name(),
          field.schema(), String.valueOf(datum)), e);
    }
  }

  // this does not contain string, bytes, or fixed because the datum type
  // doesn't necessarily determine the schema.
  private static ImmutableMap<Schema.Type, Schema> PRIMITIVES = ImmutableMap.<Schema.Type, Schema> builder()
      .put(Schema.Type.NULL, Schema.create(Schema.Type.NULL))
      .put(Schema.Type.BOOLEAN, Schema.create(Schema.Type.BOOLEAN))
      .put(Schema.Type.INT, Schema.create(Schema.Type.INT))
      .put(Schema.Type.LONG, Schema.create(Schema.Type.LONG))
      .put(Schema.Type.FLOAT, Schema.create(Schema.Type.FLOAT))
      .put(Schema.Type.DOUBLE, Schema.create(Schema.Type.DOUBLE))
      .build();

  @VisibleForTesting
  UnionResolution resolveUnion(JsonNode datum, Collection<Schema> unionSchemas) {
    if (log.isDebugEnabled()) {
      log.debug("Resolving union of types: {}",
          unionSchemas.stream().map(Schema::getName).collect(Collectors.joining(",")));
    }
    Set<Schema.Type> primitives = Sets.newHashSet();
    List<Schema> others = Lists.newArrayList();
    for (Schema unionBranchSchema : unionSchemas) {
      if (PRIMITIVES.containsKey(unionBranchSchema.getType())) {
        primitives.add(unionBranchSchema.getType());
      } else {
        others.add(unionBranchSchema);
      }
    }

    UnionResolution primitiveMatch = identifyPrimitiveMatch(datum, primitives);
    if (primitiveMatch != null) {
      return primitiveMatch;
    }

    UnionResolution otherMatch = identifyOtherMatch(datum, others);
    if (otherMatch != null) {
      return otherMatch;
    }

    return UnionResolution.NONE;
  }

  private static UnionResolution identifyPrimitiveMatch(JsonNode datum, Set<Schema.Type> primitives) {
    // Try to identify specific primitive types
    Schema primitiveSchema = null;
    if (datum == null || datum.isNull()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.NULL);
    } else if (datum.isShort() || datum.isInt()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT,
          Schema.Type.DOUBLE);
    } else if (datum.isLong()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.LONG, Schema.Type.DOUBLE);
    } else if (datum.isFloat()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.FLOAT, Schema.Type.DOUBLE);
    } else if (datum.isDouble()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.DOUBLE);
    } else if (datum.isBoolean()) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.BOOLEAN);
    }
    if (primitiveSchema == null
        && ((datum.isDouble() && datum.doubleValue() >= -Float.MAX_VALUE && datum.doubleValue() <= Float.MAX_VALUE)
            || (datum.isLong()
                && datum.longValue() >= (long) -Float.MAX_VALUE
                && datum.longValue() <= (long) Float.MAX_VALUE))) {
      primitiveSchema = closestPrimitive(primitives, Schema.Type.FLOAT, Schema.Type.DOUBLE);
    }

    if (primitiveSchema != null) {
      return new UnionResolution(primitiveSchema, MatchType.FULL);
    }
    return null;
  }

  private static Schema closestPrimitive(Set<Schema.Type> possible, Schema.Type... types) {
    for (Schema.Type type : types) {
      if (possible.contains(type) && PRIMITIVES.containsKey(type)) {
        return PRIMITIVES.get(type);
      }
    }
    return null;
  }

  private UnionResolution identifyOtherMatch(JsonNode datum, List<Schema> others) {
    // otherwise, select the first schema that matches the datum
    List<Schema> ambiguousMatches = new ArrayList<>();
    for (Schema schema : others) {
      MatchType unionMemberMatch = matches(datum, schema);

      if (unionMemberMatch == MatchType.FULL) {
        // If we find a match, we go with it
        return new UnionResolution(schema, MatchType.FULL);
      }

      if (unionMemberMatch == MatchType.AMBIGUOUS) {
        ambiguousMatches.add(schema);
      }
    }

    if (!ambiguousMatches.isEmpty()) {
      // If we only have one ambiguous match, then that is a full union match (there are no other good options)
      // Otherwise, if we have multiple ambiguous matches, we return the first.
      MatchType unionMatchType = ambiguousMatches.size() == 1 ? MatchType.FULL : MatchType.AMBIGUOUS;
      return new UnionResolution(ambiguousMatches.get(0), unionMatchType);
    }
    return null;
  }

  private MatchType matches(JsonNode datum, Schema schema) {
    switch (schema.getType()) {
    case RECORD:
      if (datum.isObject()) {
        return matchRecord(datum, schema);
      }
      break;
    case UNION:
      return resolveUnion(datum, schema.getTypes()).matchType;
    case MAP:
      if (datum.isObject()) {
        return matchMapValue(datum, schema);
      }
      break;
    case ARRAY:
      if (datum.isArray()) {
        return matchArrayElement(datum, schema);
      }
      break;
    case BOOLEAN:
      if (datum.isBoolean()) {
        return MatchType.FULL;
      }
      break;
    case FLOAT:
      if (datum.isDouble() && datum.doubleValue() >= -Float.MAX_VALUE && datum.doubleValue() <= Float.MAX_VALUE
          || datum.isLong()
              && datum.longValue() >= (long) -Float.MAX_VALUE
              && datum.longValue() <= (long) Float.MAX_VALUE
          || datum.isFloat()
          || datum.isInt()) {
        return MatchType.FULL;
      }
      break;
    case DOUBLE:
      if (datum.isDouble() || datum.isFloat() || datum.isLong() || datum.isInt()) {
        return MatchType.FULL;
      }
      break;
    case INT:
      if (datum.isInt()) {
        return MatchType.FULL;
      }
      break;
    case LONG:
      if (datum.isLong() || datum.isInt()) {
        return MatchType.FULL;
      }
      break;
    case STRING:
      if (datum.isTextual()) {
        return MatchType.FULL;
      }
      break;
    case ENUM:
      if (datum.isTextual() && schema.hasEnumSymbol(datum.textValue())) {
        return MatchType.FULL;
      }
      break;
    case BYTES:
    case FIXED:
      if (datum.isTextual()) {
        return MatchType.FULL;
      }
      break;
    case NULL:
      if (datum.isNull()) {
        return MatchType.FULL;
      }
      break;
    default: // unknown
      throw new IllegalArgumentException("Unsupported schema: " + schema);
    }
    return MatchType.NONE;
  }

  @VisibleForTesting
  MatchType matchRecord(JsonNode objectDatum, Schema schema) {
    boolean partiallyMatchedFields = false;
    Set<String> jsonFields = new HashSet<>();
    Iterators.addAll(jsonFields, objectDatum.fieldNames());

    for (Schema.Field field : schema.getFields()) {
      log.debug("Attempting to match field '{}.{}'...", schema.getName(), field.name());

      if (!objectDatum.has(field.name())) {
        log.debug("Field '{}.{}' not present in JsonNode, considering implicit values...", schema.getName(),
            field.name());
        if (field.defaultVal() != null) {
          log.debug("Absent JSON Field '{}.{}' possibly covered by default: '{}'",
              new Object[] { schema.getName(), field.name(), field.defaultVal() });
          partiallyMatchedFields = true;
        } else if (matches(NullNode.instance, field.schema()) == MatchType.FULL) {
          log.debug("Absent JSON Field '{}.{}' possibly covered by supported null in schema: '{}'",
              new Object[] { schema.getName(), field.name(), field.schema() });
          partiallyMatchedFields = true;
        } else {
          log.debug("Field '{}.{}' missing in JsonNode", schema.getName(), field.name());
          return MatchType.NONE;
        }
      } else {
        MatchType fieldMatchType = matches(objectDatum.get(field.name()), field.schema());
        switch (fieldMatchType) {

        case NONE:
          log.debug("Field '{}.{}' not compatible with JsonNode", schema.getName(), field.name());
          return MatchType.NONE;

        case AMBIGUOUS:
          log.debug("Inconclusive field match with JsonNode for '{}.{}'", schema.getName(), field.name());
          jsonFields.remove(field.name());
          partiallyMatchedFields = true;
          break;

        case FULL:
          log.debug("Located field '{}.{}' in JsonNode", schema.getName(), field.name());
          jsonFields.remove(field.name());
          break;

        default:
          throw new IllegalStateException("Unhandled TypeMatch: " + fieldMatchType);
        }
      }
    }

    if (UndeclaredFieldBehaviour.NO_MATCH == undeclaredFieldBehaviour && !jsonFields.isEmpty()) {
      return MatchType.NONE;
    }

    if (partiallyMatchedFields) {
      log.debug("Inconclusive match for record '{}' to JsonNode.", schema.getName());
      return MatchType.AMBIGUOUS;
    }

    log.debug("Matched record '{}' to JsonNode.", schema.getName());
    return MatchType.FULL;
  }

  @VisibleForTesting
  MatchType matchMapValue(JsonNode objectDatum, Schema schema) {
    Iterator<Entry<String, JsonNode>> objectDatumFields = objectDatum.fields();
    if (objectDatumFields.hasNext()) {
      // We only infer from the first value in the map
      return matches(objectDatumFields.next().getValue(), schema.getValueType());
    } else {
      // It may seem odd to return FULL here instead of AMBIGUOUS, but in the context of the document with an empty
      // map, it fully matches the map value type declared in the schema. Therefore we would not like to favour another
      // ambiguous match over this one.
      log.debug("No JsonNode map value to match against '{}'", schema.getValueType());
      return MatchType.FULL;
    }
  }

  @VisibleForTesting
  MatchType matchArrayElement(JsonNode arrayDatum, Schema schema) {
    if (arrayDatum.size() >= 1) {
      // We only infer from the first element in the array
      return matches(arrayDatum.get(0), schema.getElementType());
    } else {
      // It may seem odd to return FULL here instead of AMBIGUOUS, but in the context of the document with an empty
      // array, it fully matches the array type declared in the schema. Therefore we would not like to favour another
      // ambiguous match over this one.
      log.debug("No JsonNode array element to match against '{}'", schema.getElementType());
      return MatchType.FULL;
    }
  }

  /**
   * Returns whether null is allowed by the schema.
   * <p>
   * </p>
   * Copied from {@code org.kitesdk.data.spi.SchemaUtil.nullOk(Schema)}.
   *
   * @param schema a Schema
   * @return true if schema allows the value to be null
   */
  private static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema possible : schema.getTypes()) {
        if (nullOk(possible)) {
          return true;
        }
      }
    }
    return false;
  }

}
