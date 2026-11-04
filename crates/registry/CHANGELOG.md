# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/arcboxlabs/containerregistry-rs/compare/containerregistry-registry-v0.1.2...containerregistry-registry-v0.2.0) - 2026-11-04

### Added

- *(crane,gcrane)* full go-containerregistry command set
- *(registry)* remote images, indexes and layers with concurrent writes (pkg/v1/remote)
- *(registry)* [**breaking**] streaming, resumable and parallel blob transfers
