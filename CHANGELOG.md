# Jasvorno CHANGELOG

## T.B.D
* Started change log after 1.0.1 release.
* Added ability to ignore fields declared in the JSON message being converted, even if they are not declared in the schema. This is necessary for some 'can be read by' forwards compatibility scenarios, where the consumer might not have the latest schema, yet messages have been created with a new compatible schema that has declared one or more additional fields.