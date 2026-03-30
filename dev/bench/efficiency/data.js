window.BENCHMARK_DATA = {
  "lastUpdate": 1774898315716,
  "repoUrl": "https://github.com/strawgate/memagent",
  "entries": {
    "Efficiency": [
      {
        "commit": {
          "author": {
            "name": "strawgate",
            "username": "strawgate",
            "email": "williamseaston@gmail.com"
          },
          "committer": {
            "name": "strawgate",
            "username": "strawgate",
            "email": "williamseaston@gmail.com"
          },
          "id": "9f3a39116174933a28f5fb6bef967c7c7c0aa352",
          "message": "fix: Add github-token to benchmark-action steps\n\nThe benchmark-action requires github-token when auto-push is enabled.\nWithout it the step fails with: 'auto-push' is enabled but\n'github-token' is not set.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-03-30T18:12:52Z",
          "url": "https://github.com/strawgate/memagent/commit/9f3a39116174933a28f5fb6bef967c7c7c0aa352"
        },
        "date": 1774894925572,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "passthrough/logfwd (binary) ms/M-lines",
            "value": 2000,
            "unit": "ms",
            "extra": "n=2"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "strawgate",
            "username": "strawgate",
            "email": "williamseaston@gmail.com"
          },
          "committer": {
            "name": "strawgate",
            "username": "strawgate",
            "email": "williamseaston@gmail.com"
          },
          "id": "ecc68b9d6902e262019da16bc8bfcd762f78c538",
          "message": "fix: Clean up micro benchmark output in issue body\n\nStrip ANSI escape codes and cargo compile output from criterion\nresults. Only keep benchmark result lines (time/throughput) and\nwrap in a markdown code block.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-03-30T18:24:31Z",
          "url": "https://github.com/strawgate/memagent/commit/ecc68b9d6902e262019da16bc8bfcd762f78c538"
        },
        "date": 1774895631914,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "passthrough/logfwd (binary) ms/M-lines",
            "value": 2000,
            "unit": "ms",
            "extra": "n=2"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "strawgate",
            "username": "strawgate",
            "email": "williamseaston@gmail.com"
          },
          "committer": {
            "name": "strawgate",
            "username": "strawgate",
            "email": "williamseaston@gmail.com"
          },
          "id": "566f59769957451fc0bab2b4ed09f59920f779bd",
          "message": "fix: Tighten micro benchmark grep to name+time+throughput only\n\nPrevious grep was too broad, matching \"Warming up\", \"Collecting\",\n\"Analyzing\" lines. Now only captures benchmark name, time, and\nthroughput summary lines.\n\nCo-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>",
          "timestamp": "2026-03-30T18:55:50Z",
          "url": "https://github.com/strawgate/memagent/commit/566f59769957451fc0bab2b4ed09f59920f779bd"
        },
        "date": 1774898315241,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "passthrough/logfwd (binary) ms/M-lines",
            "value": 2500,
            "unit": "ms",
            "extra": "n=2"
          }
        ]
      }
    ]
  }
}