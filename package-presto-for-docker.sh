# Build Presto server and CLI
threads=$(nproc)
echo build presto server and cli using ${threads} threads
mvn -pl '!:presto-docs,!:presto-spark,!:presto-server-rpm,!:presto-verifier,!:presto-benchto-benchmarks,!:presto-spark-package,!:presto-product-tests' -T $threads clean package -Pdist -DskipTests -Dmaven.javadoc.skip=true -Dair.check.skip-all=true -e

VERSION=$(cat ./pom.xml | gawk 'match($0, /<version>(.*)-SNAPSHOT<\/version>/, a) {print a[1]}')
echo "copy presto-server.tar.gz and presto-cli.jar with version ${VERSION}"
cp presto-server/target/presto-server-${VERSION}-SNAPSHOT.tar.gz Docker/presto-server.tar.gz
cp presto-cli/target/presto-cli-${VERSION}-SNAPSHOT-executable.jar Docker/presto-cli.jar

