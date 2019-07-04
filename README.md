# Test Avril 2018

### Configuration requise

- [Intellij idea](https://www.jetbrains.com/idea/download/) + plugin scala activé

- Jdk8 / Maven

### Objectifs

Nous montrer un aperçu de votre façon de programmer, que vous êtes - ou pouvez être - à l'aise aussi bien 
avec la jvm, spark que aws ou github. 

A faire décontracté, en s'appuyant sur les documentations des librairies utilisées dans le projet si vous n'êtes pas 
familier avec celles-ci. Mais sans pour autant piocher sur stackoverflow.

#### Github
1. Forker le projet

2. Faire une pull request dès que vous avez terminé

#### Spark / scala

Créer un référentiel au format Apache Parquet contenant le classement général des 5 grands championnats de football
européen sur les 40 dernières années en grapillant les informations sur wikipedia 
(ex: [ici](https://fr.wikipedia.org/wiki/Championnat_de_France_de_football_2010-2011)).   
```html
<table class="wikitable gauche" style="text-align:center; line-height:16px;">
  <caption>Classement</caption>
  <tr bgcolor="#F2F2F2">
    <th scope="col" style="">Rang</th>
    <th scope="col" style="" width="200">Équipe</th>
    <th scope="col" style="width:20px"><span style="cursor:help;" title="Points">Pts</span></th>
    <th scope="col" style="width:20px"><span style="cursor:help;" title="Matchs joués">J</span></th>
    <th scope="col" style="width:20px;border-right-style:hidden"><span style="cursor:help;" title="Matchs gagnés">G</span></th>
    <th scope="col" style="width:20px;border-right-style:hidden"><span style="cursor:help;" title="Matchs nuls">N</span></th>
    <th scope="col" style="width:20px"><span style="cursor:help;" title="Matchs perdus">P</span></th>
    <th scope="col" style="width:20px;border-right-style:hidden"><span style="cursor:help;" title="Buts pour">Bp</span></th>
    <th scope="col" style="width:20px;border-right-style:hidden"><span style="cursor:help;" title="Buts contre">Bc</span></th>
    <th scope="col" style="width:25px"><span style="cursor:help;" title="Différence de buts">Diff</span></th>
  </tr>
  <tr bgcolor="white">
    <td><b><span class="nowrap">1</span></b></td>
    <td align="left"><span class="nowrap"><a href="/wiki/Lille_OSC" class="mw-redirect" title="Lille OSC">Lille</a></span></td>
    <td><b>44</b></td>
    <td>19</td>
    <td style="border-right-style:hidden">13</td>
    <td style="border-right-style:hidden">5</td>
    <td>1</td>
    <td style="border-right-style:hidden">40</td>
    <td style="border-right-style:hidden">17</td>
    <td>+23</td>
  </tr>
  ...
```

Il faudra extraire les données contenues dans les differents tableaux html et créer un export partquet ayant cette forme. 

```text
+-------+------+--------+-------------+------+------+---+-----+----+--------+------------+---------------+
| league|season|position|         team|points|played|won|drawn|lost|goalsFor|goalsAgainst|goalsDifference|
+-------+------+--------+-------------+------+------+---+-----+----+--------+------------+---------------+
|Ligue 1|  2003|      10|         Nice|    55|    38| 13|   16|   9|      39|          31|              8|
|Ligue 1|  2008|      11|         Caen|    51|    38| 13|   12|  13|      48|          53|             -5|
|Ligue 1|  1979|       1|RC Strasbourg|    56|    38| 22|   12|   4|      68|          28|             40|
```  

##### Comment faire

Créer le projet dans Intellij Idea en important la configuration maven à la racine. 

Toutes les librairies sont fournies pour la réalisation du test.

Le test unitaire _com.test.spark.wiki.extracts.RunTasks_ permet de lancer les 2 tâches spark suivantes:

- com.test.spark.wiki.extracts.Q1_WikiDocumentsToParquetTask
  - La classe contient des petites questions générales
  - Cette classe genère les urls à crawler depuis le fichier _src/main/resources/leagues.yaml_
  - La classe ne compile pas
  - Il manque le parsing des pages wikipedia avec la librairie Jsoup
  - Il manque "l'activation" de spark
  - Générer des données en respectant le schéma de la classe _LeagueStanding_
  
- com.test.spark.wiki.extracts.Q2_ShowLeagueStatsTask
  - En utilisant les données générées par _Q1_WikiDocumentsToParquetTask_ répondre aux questions posées dans la classe
  
La commande `mvn test` doit permettre de voir les résultats attendus

#### Aws
- Ecrire un fichier cloudformation yaml dans _cfn/s3.yml_ permettant de monter
  - Un bucket S3 "A"
  - Un bucket S3 "B" contenant les logs d'accès du bucket S3 "A"
  
- S'il fallait protéger les données sur un bucket S3 comment procéderiez-vous ?



Bonne session :)
