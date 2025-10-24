# Changelog

All notable changes to the Retail Data Generator project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Comprehensive Streaming Documentation Suite**
  - `docs/STREAMING_SETUP.md` - Complete setup guide with Azure Event Hub and Microsoft Fabric RTI configuration
  - `docs/STREAMING_API.md` - Full REST API reference with all 15 event types and payloads documented
  - `docs/STREAMING_OPERATIONS.md` - Production operations guide with monitoring, troubleshooting, and performance tuning
  - `docs/CREDENTIALS.md` - Security best practices for credential management with Azure Key Vault integration
  - New streaming section added to main README.md with quick start guide and feature overview
  - All event envelope schemas and payload structures fully documented
  - Supply chain disruption API endpoints documented with examples
  - Circuit breaker, dead letter queue, and health check endpoints detailed
  - CI/CD integration examples for GitHub Actions, Azure DevOps, and GitLab
  - Kubernetes deployment manifests and Docker configuration examples
  - Prometheus alerting rules and monitoring script examples

- **Enhanced Progress Display for Historical Data Generation**
  - Table completion counter displaying "X/8 tables complete" in real-time
  - Estimated time remaining (ETA) calculation and display (e.g., "~2 minutes")
  - Progress update throttling with 100ms minimum interval to prevent API flooding
  - New optional API response fields in `GenerationStatusResponse`:
    - `tables_in_progress: list[str] | None` - List of tables currently being generated
    - `estimated_seconds_remaining: float | None` - Approximate seconds until completion
    - `progress_rate: float | None` - Progress velocity as rolling average
    - `last_update_timestamp: str | None` - ISO-8601 timestamp of last progress update
  - Comprehensive test coverage with 66 total tests (51 unit + 15 integration)
  - Mobile-friendly responsive design for progress display

### Changed
- `GenerationStatusResponse` API model enhanced with optional progress tracking fields
- Frontend JavaScript (`static/js/main.js`) now displays table counter and ETA
- UI template (`templates/index.html`) updated with new progress elements
- Progress updates throttled in `FactDataGenerator` to improve UX during fast generation
- CSS styling (`static/css/style.css`) updated for better progress visualization

### Technical Details
- Added `ProgressThrottler` mechanism in `FactDataGenerator` for controlled update frequency
- Implemented ETA calculation based on rolling 10-update history window
- Table state tracking with three phases: `not_started` → `in_progress` → `completed`
- All new API fields validated with Pydantic constraints (e.g., `ge=0.0` for numeric values)
- Progress rate calculated as moving average to smooth out generation speed variations

### Backward Compatibility
- All new API fields are optional with `None` defaults
- Existing code creating `GenerationStatusResponse` objects continues to work without modification
- No breaking changes to existing endpoints or data contracts
- Old API responses remain valid and functional

---

## Historical Releases

_Previous releases will be documented here as the project evolves._

### Note on Versioning
This project follows semantic versioning:
- **MAJOR**: Incompatible API changes or breaking changes to data contracts
- **MINOR**: New features added in a backward-compatible manner
- **PATCH**: Backward-compatible bug fixes and minor improvements
