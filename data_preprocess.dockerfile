FROM gaddamsrikanth24/house-prices

ENV SPARK_HOME=/usr/local/spark

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin

COPY configurations.txt ./configurations.txt

COPY data_preprocess.py ./data_preprocess.py

CMD ["python3", "data_preprocess.py"]