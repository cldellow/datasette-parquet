#!/bin/bash
set -euo pipefail

datasette --reload --metadata metadata.json
