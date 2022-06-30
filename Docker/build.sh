#!/bin/bash
sudo echo "got root access"

buildConfigs() {
    rm -rf .configs && mkdir .configs
    for CONF in cpp-coordinator java-coordinator java-worker; do
        rm -rf .configs/${CONF}-config && mkdir .configs/${CONF}-config;
        cp -r configs/catalog .configs/${CONF}-config/;
        cp configs/${CONF}-config.properties .configs/${CONF}-config/config.properties;
        cp configs/${CONF}-node.properties .configs/${CONF}-config/node.properties;
        cp configs/log.properties .configs/${CONF}-config/;
    done;
    cp configs/jvm-coordinator.config .configs/java-coordinator-config/jvm.config;
    cp configs/jvm-worker.config .configs/java-worker-config/jvm.config;
}

buildConfigs;

#copy configs to presto cpp worker
rm -rf ~/presto/presto_cpp_container/ctx/worker_config/catalog
cp -r configs/catalog ~/presto/presto_cpp_container/ctx/worker_config/

docker build -t presto:cpp-coordinator --build-arg CONFIG=cpp-coordinator-config -f Dockerfile .
docker build -t presto:java-coordinator --build-arg CONFIG=java-coordinator-config -f Dockerfile .
docker build -t presto:java-worker --build-arg CONFIG=java-worker-config -f Dockerfile .

