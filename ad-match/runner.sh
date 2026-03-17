#!/usr/bin/env bash
##############################################
# Runner Script for deploying samza job    ###
##############################################

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Auto-detect HDFS namenode from Hadoop configuration
NAMENODE=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo "hdfs://localhost:8020")
echo "🔍 Detected HDFS namenode: $NAMENODE"

# Preparing folder for deployment
mkdir -p deploy/samza/config

# Compile and build the jar
mvn clean package
rm -rf deploy/samza/*

# Extract tar.gz file to deployment folder
tar -xvf target/nycabs-0.0.1-dist.tar.gz -C deploy/samza/

# CRITICAL FIX: Copy your config file to the expected location
cp src/main/config/ad-match.properties deploy/samza/config/ad-match.properties

# 🔥 ULTIMATE Java 11 + Jackson FIXES (fixes ALL containers)
cd deploy/samza

# Fix ALL shell scripts for Java 11
find bin/ -name "*.sh" -exec sed -i 's/-d64//g' {} \;
find bin/ -name "*.sh" -exec sed -i 's/-XX:PrintGCDateStamps//g' {} \;
find bin/ -name "*.sh" -exec sed -i 's/-XX:+PrintGCDateStamps//g' {} \;
find bin/ -name "*.sh" -exec sed -i 's/-XX:+UseConcMarkSweepGC//g' {} \;
find bin/ -name "*.sh" -exec sed -i '103s/.*src.*/echo "GC logging fixed"/' {} \; 2>/dev/null || true

# Fix SLF4J multiple bindings
rm -f lib/slf4j-log4j12-*.jar lib/slf4j-reload4j-*.jar 2>/dev/null || true

# 🔥 CRITICAL: Fix Jackson version mismatch
# Remove ALL old Jackson jars and download matching 2.15.2 versions
echo "Removing old Jackson libraries..."
rm -f lib/jackson-core-2.12*.jar lib/jackson-databind-2.12*.jar lib/jackson-annotations-2.12*.jar
rm -f lib/jackson-core-2.15*.jar lib/jackson-databind-2.15*.jar lib/jackson-annotations-2.15*.jar

echo "Downloading Jackson 2.15.2 libraries..."
wget -q -O lib/jackson-core-2.15.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar
wget -q -O lib/jackson-databind-2.15.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar
wget -q -O lib/jackson-annotations-2.15.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar

echo "✅ ALL Java 11 and Jackson fixes applied!"
echo "Fixed scripts: $(find bin/ -name '*.sh' | wc -l)"
echo "Jackson libraries:"
ls -lh lib/jackson-*.jar | grep "2.15"

# 🔥 CREATE FIXED PACKAGE - tar from INSIDE deploy/samza (critical for correct structure)
echo "Creating fixed package..."
tar -czf ../../target/nycabs-0.0.1-fixed.tar.gz .

# Verify the structure
echo "Verifying package structure:"
tar -tzf ../../target/nycabs-0.0.1-fixed.tar.gz | head -10

# Go back to project root
cd "$SCRIPT_DIR"

# Upload FIXED package to HDFS
echo "Uploading fixed package to HDFS..."
hadoop fs -rm -f /nycabs-0.0.1-fixed.tar.gz 2>/dev/null || true
hadoop fs -copyFromLocal target/nycabs-0.0.1-fixed.tar.gz /

# Update config to point to the fixed package with auto-detected namenode
echo "Updating config with detected namenode: $NAMENODE"
sed -i "s|yarn.package.path=.*|yarn.package.path=$NAMENODE/nycabs-0.0.1-fixed.tar.gz|" deploy/samza/config/ad-match.properties

echo "Updated config file:"
cat deploy/samza/config/ad-match.properties | grep yarn.package.path

# 🔥 RUN SAMZA JOB
deploy/samza/bin/run-app.sh \
  --config-path="$SCRIPT_DIR/deploy/samza/config/ad-match.properties"

echo "🚀 Job launched! Check status:"
sleep 5
yarn application -list -appStates RUNNING,SUBMITTED,ACCEPTED
