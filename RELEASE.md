# Release Process

Simple guide for releasing new versions of dagster-odp.

## Creating a Release

1. Create a release branch
   ```bash
   git checkout -b release/0.1.0
   ```

2. Update version in `pyproject.toml`
   ```toml
   [project]
   version = "0.1.0"
   ```

3. Update `CHANGELOG.md`
   ```markdown
   # Changelog

   ## [0.1.0] - YYYY-MM-DD
   - List your changes here
   ```

4. Create a PR to merge into `main`
   - Title: "Release 0.1.0"
   - Get review and approval

5. Merge PR
   - The release workflow will automatically:
     - Create a GitHub release
     - Build and upload the package

## Versioning

We use [Semantic Versioning](https://semver.org/):
- MAJOR.MINOR.PATCH (e.g., 0.1.0)
- Increment MAJOR for breaking changes
- Increment MINOR for new features
- Increment PATCH for bug fixes