FROM openjdk:21-jdk-slim
WORKDIR /

COPY target/DataGeneration-1.0-SNAPSHOT.jar app.jar


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    bash \
    jq \
    coreutils \
    findutils \
    gawk \
    && rm -rf /var/lib/apt/lists/*

COPY config.json /app/config/config.json
COPY splitInsertsAndDeletesODC.sh /app/splitInsertsAndDeletesODC.sh

#RUN javac src/main/java/org/*.java

RUN chmod +x /app/splitInsertsAndDeletesODC.sh

CMD bash -c "\
echo 'Step 1: Create dataset and queries' &&\
java -cp app.jar org.CreateDatasetAndQueries /app/config/config.json &&\
echo 'Step 2: Split inserts and deletes' &&\
/app/splitInsertsAndDeletesODC.sh &&\
echo 'Step 3: Mix into final file' &&\
java -cp app.jar org.MixInMemory /app/config/config.json &&\
echo 'Done!'"

#
#ENV CLASSPATH=src
#
#CMD bash -c "\
#echo 'Step 1: Create dataset and queries' &&\
#java org.CreateDatasetAndQueries config.json &&\
#echo 'Step 2: Split inserts and deletes' &&\
#./splitInsertsAndDeletesODC.sh &&\
#echo 'Step 3: Mix into final file' &&\
#java org.MixInMemory config.json &&\
#echo 'Done!'\
#"

LABEL authors="wieger"
