# package driver code for benchmarking (available as git submodule)
(
cd Bao-for-Presto && \
    tar -czvf driver.tar.gz *.py queries requirements.txt schema.sql benchmark.sh && \
    mv driver.tar.gz ../Docker/
)

threads=24
mvn -pl '!:presto-docs,!:presto-spark,!:presto-server-rpm,!:presto-verifier,!:presto-benchto-benchmarks,!:presto-spark-package,!:presto-product-tests' -T $threads clean package -Pdist -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true -e

echo "copy presto-server.tar.gz and presto-cli.jar"
VERSION=0.274
cp presto-server/target/presto-server-${VERSION}-SNAPSHOT.tar.gz Docker/presto-server.tar.gz
cp presto-cli/target/presto-cli-${VERSION}-SNAPSHOT-executable.jar Docker/presto-cli.jar
