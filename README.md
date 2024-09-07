# Installation

Setup pip env :
```
sudo apt install python3.9-venv
python3 -m venv .venv
source .venv/bin/activate
```

Install requirements :
```
pip3 install -r requirements.txt
pyspark --packages org.apache.hadoop:hadoop-aws:3.3.2
exit()
```

Copy spark/hadoop jars to .venv/lib/python3.8/site-packages/pyspark/jars (get from https://mvnrepository.com):
```
aws-java-sdk-bundle-1.11.874.jar
hadoop-aws-3.3.4.jar

run this command at the root of the project on linux or mac (ensure the venv is named .venv):
sh helpers/libs.sh
```

## Launch tests

``` 
python3 -m test.<test_filename>
```

This test regenerate territories for MWN usecase : 
```
python3 -m test.test_generate_ter
```

## Launch main

From Linux/WSL :
```
python3 main.py
```

If using S3 export AWS credentials in the terminal.

If connecting to RDS use .env file.

.venv/lib/python3.10/site-packages/pyspark/jars


## Test Mapping

### Exigences couvertes par test/test_mwn_restitution | test_mwn_expected
Cas MWN Hello, Candy Necklace, Fortnight [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/490340360/Use+Case+MWN+-++SETUP)

### Exigences couvertes par test/test_spec_lot1 | test_spec_lot1_expected :

Calculer les Ayants Droit Clés avec un contrat applicable
* 001 Sur une œuvre avec un seul lettrage ou sans lettrage [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-une-%C5%93uvre-avec-un-seul-lettrage-ou-sans-lettrage-Cas-existant)
* 002 Sur une œuvre avec plusieurs lettrages [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-une-%C5%93uvre-avec-plusieurs-lettrages-Cas-existant)
* 003 Sur une œuvre avec une branche où un seul ayant droit est gestionnaire [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-une-%C5%93uvre-avec-une-branche-o%C3%B9-un-seul-ayant-droit-est-gestionnaire-Cas-existant)
* 004 Sur une œuvre avec une branche où tous les ayants droit sont gestionnaires [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-une-%C5%93uvre-avec-une-branche-o%C3%B9-tous-les-ayants-droit-sont-gestionnaires-Cas-existant)
* 005 Sur une œuvre avec une branche où aucun ayant droit est gestionnaire [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-une-%C5%93uvre-avec-une-branche-o%C3%B9-aucun-ayant-droit-est-gestionnaire-CAS-existant)

Calculer les Ayants Droit Clés avec des contrats portant des restrictions
* Sur le pays d’utilisation [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-le-pays-d%E2%80%99utilisation-cas-existant)
* Sur les personnes
  * de type stricte inclusive où le contrat ne s’applique pas [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#de-type-stricte-inclusive-o%C3%B9-le-contrat-ne-s%E2%80%99applique-pas-cas-existant)
  * de type stricte inclusive où le contrat s’applique [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#de-type-stricte-inclusive-o%C3%B9-le-contrat-s%E2%80%99applique---cas-existant)
  * de type stricte exclusive [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#de-type-stricte-exclusive-cas-)
  * de type non stricte exclusive [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#de-type-non-stricte-exclusive-cas-existant)
  * de type non stricte inclusive [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/512950286/Calculer+les+Ayants+Droit+Cl+s+avec+des+contrats+portant+des+restrictions#de-type-non-stricte-inclusive-CAS-EXISTANT)
* Sur la famille d'utilisation [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-le-famille-d%E2%80%99utilisation-cas-existant)
* Sur le Genre de l'oeuvre [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Sur-le-genre-d%E2%80%99%C5%93uvre--Cas-existant)
* Sur le code oeuvre
  * inclusives [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/edit-v2/393478155#de-type-inclusive--cas-existant)
  * exclusives [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/edit-v2/393478155#de-type-exclusive-cas-existant)

Restitution avec plusieurs contrats simultanément [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#Avec-plusieurs-contrats-simultan%C3%A9ment-Cas-existant) \
En fonction de la date consultation [lien](https://docsacem.atlassian.net/wiki/spaces/OCTAVE/pages/393478155/Restituer+les+Ayants+Droit+et+les+cl+s+d+une+oeuvre#En-fonction-de-la-date-consultation-Cas-existant)


## Lancer un job EMR
Docker login \
Si changements dans le code : nécessaire de repusher une image docker \
Point d'entrée [emr_determination_chaine_contrat.py](emr_determination_chaine_contrat.py) \
Configuration [emr_config_contrat.json](emr_config_contrat.json)

```
<Exporter les variables AWS>
docker login --username AWS -p $(aws ecr get-login-password --region eu-west-1) 806535937423.dkr.ecr.eu-west-1.amazonaws.com
docker build -t sacem-perfo-ecr-custom-dev .
docker tag sacem-perfo-ecr-custom-dev:latest 806535937423.dkr.ecr.eu-west-1.amazonaws.com/sacem-perfo-ecr-custom-dev:latest
docker push 806535937423.dkr.ecr.eu-west-1.amazonaws.com/sacem-perfo-ecr-custom-dev:latest
aws emr-containers start-job-run --cli-input-json file://./emr_config_contrat.json
to stop a job: aws emr-containers cancel-job-run --virtual-cluster-id <cluster-id> --id <job-id>
```
