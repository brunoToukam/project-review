FROM 483788554619.dkr.ecr.eu-west-1.amazonaws.com/spark/emr-7.1.0:latest
USER root

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

USER hadoop:hadoop
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar /opt/spark/jars/
WORKDIR /perfo_workdir
RUN mkdir -p /perfo_workdir/computed

COPY data_loader ./data_loader
COPY utils ./utils
#COPY data ./data

COPY emr_determination_chaine_contrat.py generate_ter.py __init__.py ./
#  main.py get_jeu_cle_statique.py get_restitution.py settings.py
