# Jasvorno CHANGELOG

## [1.1.1] - 2017-11-21
### Changed
* Build settings (to test release builds)

## [1.1.0] - 2017-11-20
### Added
* Validating of recursive schemas (previously failed with a stack overflow). 
* Added cast to the float value in check for long in float range.

### Changed
* Corrected check for ensuring byte length is equal to the fixed size schema.
* Corrected lower boundary of float range - changed Float.MIN to -Float.MAX.

## [1.0.1] - 2017-03-27
* Started change log at 1.0.1 release.
* Added ability to ignore fields declared in the JSON message being converted, even if they are not declared in the schema. This is necessary for some 'can be read by' forwards compatibility scenarios, where the consumer might not have the latest schema, yet messages have been created with a new compatible schema that has declared one or more additional fields.
