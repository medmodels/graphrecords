# Changelog (Python)

All notable changes to the Python package will be documented in this file.

## [0.6.1] - 2026-08-31

### Features

- enable creating node and edge batches from any source (#84)

## [0.6.0] - 2026-08-31

### Breaking Changes

- refactoring of graphrecord storage and interface (#80)

### Refactoring

- refactoring of graphrecord storage and interface (#80)

## [0.5.0] - 2026-08-10

### Breaking Changes

- query engine typing inconsistencies (#77)
- complete query engine rework (#70)

### Bug Fixes

- query engine typing inconsistencies (#77)
- Missing type in QueryResult (#65)
- align EdgeIndexer `__delitem__` no-match query behavior with NodeIndexer (#61)

### Refactoring

- complete query engine rework (#70)
- Pick changes from #57 (#63)

## [0.4.1] - 2026-04-07

### Features

- implement hash eq for GraphRecordValue (#51)
- enable adding nodes and edges to multiple groups at a time (#48)

## [0.4.0] - 2026-03-16

### Breaking Changes

- implement graphrecord connectors (#43)
- add missing builder functionality (#39)

### Features

- implement graphrecord connectors (#43)
- add missing builder functionality (#39)

## [0.3.0] - 2026-03-09

### Breaking Changes

- add ability to add and remove plugins (#36)

### Features

- add ability to add and remove plugins (#36)

## [0.2.0] - 2026-03-03

### Documentation

- Add AI_POLICY (#23)

### Features

- Move plugin system to rust (#20)
- Add plugin functionality (#15)

### Refactoring

- Restructure rust code (#27)
- Switch to parking_lot and  other minor refactorings (#19)
