# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/arcboxlabs/containerregistry-rs/compare/containerregistry-v0.1.2...containerregistry-v0.2.0) - 2026-11-04

### Added

- *(crane,gcrane)* full go-containerregistry command set
- *(layout)* v1 image/index source and sink, append/replace/remove, gc (pkg/v1/layout)
- *(registry)* remote images, indexes and layers with concurrent writes (pkg/v1/remote)
- *(v1)* image, index and layer model with mutate, tarball, empty, random, partial, stream, validate and cache
- *(server)* in-memory and disk-backed OCI registry (pkg/registry)
- *(registry)* [**breaking**] streaming, resumable and parallel blob transfers

### Other

- re-export v1 and server from the facade crate, document streaming and parity
