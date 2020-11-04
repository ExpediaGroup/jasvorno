# Jasvorno CHANGELOG

## [TBD] - TBD
### Changed
- Upgraded `jackson` version to 2.11.3 (was 2.9.9).
- Upgraded `junit` version to 4.13.1 (was 4.12).
- Upgraded `pitest` version to 1.5.2 (was 1.2.4).
- Upgraded `guava` version to 30.0-jre (was 20.0).
- Upgraded `hotels-oss-parent` version to 6.2.0 (was 2.0.5).
- Upgraded `maven-shade-plugin` version to 3.2.4 (was 2.4.3).

## [1.1.2] - 2019-07-17
### Changed
- Upgraded `jackson` version to 2.9.9 (was 2.7.4).

## [1.1.1] - 2017-11-21
### Changed
- Build settings (to test release builds)

## [1.1.0] - 2017-11-20
### Added
- Validating of recursive schemas (previously failed with a stack overflow). 
- Added cast to the float value in check for long in float range.

### Changed
- Corrected check for ensuring byte length is equal to the fixed size schema.
- Corrected lower boundary of float range - changed Float.MIN to -Float.MAX.

## [1.0.1] - 2017-03-27
- Started change log at 1.0.1 release.
- Added ability to ignore fields declared in the JSON message being converted, even if they are not declared in the schema. This is necessary for some 'can be read by' forwards compatibility scenarios, where the consumer might not have the latest schema, yet messages have been created with a new compatible schema that has declared one or more additional fields.
