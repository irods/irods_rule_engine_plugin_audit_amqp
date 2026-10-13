# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project **only** adheres to the following _(as defined at [Semantic Versioning](https://semver.org/spec/v2.0.0.html))_:

> Given a version number MAJOR.MINOR.PATCH, increment the:
> 
> - MAJOR version when you make incompatible API changes
> - MINOR version when you add functionality in a backward compatible manner
> - PATCH version when you make backward compatible bug fixes

## [5.0.2] - 2026-10-14

This release adds support for Ubuntu 26.04.

### Changed

- Remove installation of unused nanodbc package dependency from build hook (#195).

### Added

- Add build hook option for compiling against specific version of release package (irods/irods_development_environment#165).
