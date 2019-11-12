# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [0.1.0] - 2019-11-12

### Added
- Config option added: ckanext.downloadall.dataset_fields_to_add_to_datapackage for including custom fields from the dataset in the datapackage.json

### Fixed
- Fixed home page exception KeyError: 'resources'

## [0.0.2] - 2019-06-30
### Added
- Command-line interface.
- Schema added to the datapackage.json if a resource's Data Dictionary is completed.

### Changed
- Dependencies moved to setup.py's install_requires, for convenience during install.

### Fixed
- Fixed exception when non-download-all jobs are put on the CKAN background task queue.
- Fixed position of the "Download all" button to avoid overlapping bottom edge when no dataset.notes.
- Zip resource is not now shown in the sidebar resources on the resource preview page.
- Zip format is not now shown in the search facets for the Download All zip.
- Fix updating the zip when changes are made to the core dataset metadata (e.g. dataset title).

## [0.0.1] - 2019-05-27 - Initial release
### Added
- Generates a zip when a resource URL changes
- Zip contains resources and basic datapackage.json
- 'Download all' button placed on the dataset page
