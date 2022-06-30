#! /bin/bash

JAVA_COORD_CONTAINER_NAME=presto_java_coord_docker;
JAVA_WORKER_CONTAINER_NAME=presto_java_worker_docker;
CPP_COORD_CONTAINER_NAME=presto_cpp_coord_docker;
CPP_WORKER_CONTAINER_NAME=presto_cpp_worker_docker;


stop_container() {
    docker stop $1
    docker rm $1
}

stop_all() {
    stop_container ${JAVA_WORKER_CONTAINER_NAME};
    stop_container ${JAVA_COORD_CONTAINER_NAME};
    stop_container ${CPP_COORD_CONTAINER_NAME};
    stop_container ${CPP_WORKER_CONTAINER_NAME};
}


PS3='choose setup to run: '
read -n 1 -p "[1] JAVA    [2] CPP     [q] Quit" ans;
case $ans in
    1)
        echo "JAVA";
        stop_all;

        docker run -d --network="host" --name ${JAVA_COORD_CONTAINER_NAME} -v presto_hive_data:/volume -v presto_tmp:/tmp presto:java-coordinator;
        docker run -d --network="host" --name ${JAVA_WORKER_CONTAINER_NAME} -v presto_hive_data:/volume -v presto_tmp:/tmp presto:java-worker;
        ;;
    2)
        echo "CPP"
        stop_all;

        docker run -d --network="host" --name ${CPP_COORD_CONTAINER_NAME} -v presto_hive_data:/mnt/space/presto-data presto:cpp-coordinator;
        docker run -d --network="host" --name ${CPP_WORKER_CONTAINER_NAME} -v presto_hive_data:/mnt/space/presto-data presto:cpp-worker;
        ;;
    q)
        echo "quit"
        break
        ;;
    *) echo "invalid option $REPLY";;
esac
