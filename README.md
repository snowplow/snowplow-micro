# Snowplow Micro

[![Docker Image Version (latest semver)][docker-image]][docker-micro]
[![Docker pulls][docker-pulls]][docker-micro]
[![Build Status][gh-actions-image]][gh-actions]
[![License][license-image]][license]

Snowplow Micro is a lightweight version of the Snowplow pipeline. It’s great for:
* Getting familiar with Snowplow
* Debugging and testing, including automated testing

Just like a real Snowplow pipeline, Micro receives, validates and enriches events sent by your tracking code.

Learn more in the documentation: https://docs.snowplow.io/docs/getting-started-with-micro/basic-usage/

---

## Maintainer quick start

First, install [Git][git], [sbt][sbt] and [npm][npm].

Then clone the repository and publish the Collector dependency locally:

```text
git clone git@github.com:snowplow-incubator/snowplow-micro.git
cd snowplow-micro

git clone --branch 3.7.0 --depth 1 git@github.com:snowplow/stream-collector.git
cd stream-collector
sbt +publishLocal && cd ..
```

To run the tests:

**Prerequisites**: PostgreSQL tests require a local PostgreSQL instance. Start one with Docker:

```bash
docker run --name micro-postgres-test \
  -e POSTGRES_PASSWORD=test_password \
  -e POSTGRES_USER=test_user \
  -e POSTGRES_DB=micro_test \
  -p 5432:5432 \
  -d postgres:15
```

Then run the tests:

```bash
sbt test
```

To stop the test database:
```bash
docker stop micro-postgres-test && docker rm micro-postgres-test
```

To build a Docker image for local testing:
```
cd ui
npm install
npm run build && cd ..
sbt docker:publishLocal
```

Note the Docker image name in the output.

## Copyright and License

Copyright (c) 2019-present Snowplow Analytics Ltd. All rights reserved.

Licensed under the [Snowplow Limited Use License Agreement][license]. _(If you are uncertain how it applies to your use case, check our answers to [frequently asked questions][faq].)_

[docker-micro]: https://hub.docker.com/r/snowplow/snowplow-micro
[docker-image]: https://img.shields.io/docker/v/snowplow/snowplow-micro?sort=semver
[docker-pulls]: https://img.shields.io/docker/pulls/snowplow/snowplow-micro
[distroless-repo]: https://github.com/GoogleContainerTools/distroless

[gh-actions]: https://github.com/snowplow-incubator/snowplow-micro/actions
[gh-actions-image]: https://github.com/snowplow-incubator/snowplow-micro/actions/workflows/test.yml/badge.svg?branch=master
[gh-releases]: https://github.com/snowplow-incubator/snowplow-micro/releases

[license]: https://docs.snowplow.io/limited-use-license-1.1
[license-image]: https://img.shields.io/badge/license-Snowplow--Limited--Use-blue.svg?style=flat
[faq]: https://docs.snowplow.io/docs/contributing/limited-use-license-faq/

[snowplow]: https://github.com/snowplow/snowplow
[discourse]: https://discourse.snowplow.io

[example-dir]: https://github.com/snowplow-incubator/snowplow-micro/tree/master/example

[iglu]: https://github.com/snowplow/iglu
[iglu-resolver-config]: https://docs.snowplow.io/docs/pipeline-components-and-applications/iglu/iglu-resolver/
[iglu-resolver-example]: https://github.com/snowplow-incubator/snowplow-micro/blob/master/example/iglu.json

[stream-collector]: https://github.com/snowplow/stream-collector
[stream-collector-config]: https://docs.snowplow.io/docs/pipeline-components-and-applications/stream-collector/configure/#basic-configuration
[collector-config-example]: https://github.com/snowplow-incubator/snowplow-micro/blob/master/example/micro.conf

[snowplow-trackers]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/
[micro-rest-api]: https://docs.snowplow.io/docs/managing-data-quality/testing-and-qa-workflows/set-up-automated-testing-with-snowplow-micro/#rest-api

[techdocs]: https://docs.snowplow.io/docs/managing-data-quality/testing-and-qa-workflows/set-up-automated-testing-with-snowplow-micro
[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[roadmap]: https://github.com/snowplow/snowplow/projects/7
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing]: https://docs.snowplow.io/docs/contributing
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[git]: https://git-scm.com/
[sbt]: https://www.scala-sbt.org/
[npm]: https://www.npmjs.com/
