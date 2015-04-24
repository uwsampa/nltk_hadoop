#!/usr/bin/env bash

PATENT_SETTINGS_FILE=/shared/patents/settings.sh
source $PATENT_SETTINGS_FILE

exec $@
