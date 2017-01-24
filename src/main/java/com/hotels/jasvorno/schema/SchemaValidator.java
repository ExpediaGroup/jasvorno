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
package com.hotels.jasvorno.schema;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

import java.util.EnumSet;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public final class SchemaValidator {
  private final static Set<Type> COMPOSITE_TYPES = EnumSet.of(RECORD, UNION, MAP, ARRAY);

  private SchemaValidator() {
  }

  /**
   * We don't allow {@code union[bytes, string[, ...]]}.
   */
  public static boolean isValid(Schema schema) {
    try {
      validate(schema);
    } catch (SchemaValidationException e) {
      return false;
    }
    return true;
  }

  /**
   * We don't allow {@code union[bytes, string[, ...]]}.
   *
   * @throws SchemaValidationException
   */
  public static void validate(Schema schema) throws SchemaValidationException {
    internalValidate(schema);
  }

  private static void internalValidate(Schema schema) throws SchemaValidationException {
    if (schema.getType() == RECORD) {
      for (Field field : schema.getFields()) {
        internalValidate(field.schema());
      }
    } else if (schema.getType() == MAP) {
      internalValidate(schema.getValueType());
    } else if (schema.getType() == ARRAY) {
      internalValidate(schema.getElementType());
    } else if (schema.getType() == UNION) {
      boolean containsBytes = false;
      boolean containsString = false;
      for (Schema unionSchema : schema.getTypes()) {
        if (unionSchema.getType() == Schema.Type.BYTES) {
          containsBytes = true;
        } else if (unionSchema.getType() == Schema.Type.STRING) {
          containsString = true;
        }
        if (containsBytes && containsString) {
          String message = "Schema contains a variant of union[bytes, string]: " + unionSchema.toString();
          throw new SchemaValidationException(message);
        }
        if (COMPOSITE_TYPES.contains(unionSchema)) {
          internalValidate(unionSchema);
        }
      }
    }
  }
}
