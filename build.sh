#!/bin/sh
set -e
mvn -B compile
mvn -B exec:java -Dexec.mainClass="biggis.exasol.websockets.ExaWebSocketTest"
