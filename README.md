# icebergray

The icebergray provides an [Apache Iceberg](https://iceberg.apache.org/) table reader for the [Ray](https://www.ray.io/) open-source ML toolkit.

## Environment Installation
### Linux/Mac:
```bash
make install
```
### Mac(Apple Silicon):
```bash
make install-apple-silicon
```

## Integration Tests
```bash
make test-integration
```
Thanks to [pyiceberg](https://github.com/apache/iceberg/tree/master/python/dev) for providing the integration test framework.