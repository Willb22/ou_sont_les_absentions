# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-07-23

### Added
- 3 containerised services: Database, ETL and Flask web application
- AWS deployment via Terraform (EC2, Route53, S3 for TLS certs)
- Seperate Dev, Pre-Prod and Prod environments 
- All 3 services deployed on a single EC2 for Pre-Prod and Prod   
