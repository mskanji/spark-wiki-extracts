package com.test.spark.wiki.extracts


import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.slf4j.{Logger, LoggerFactory}


  case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  System.setProperty("hadoop.home.dir","C:\\hadoop\\hadoop-2.8.0\\bin" )

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)
    import session.implicits._
    val result1 = getLeagues
      // TODO Q1 Transformer cette seq en dataset
      .toDS()
      .flatMap {
        input =>
          (fromDate.getYear until toDate.getYear).map {
            year =>
              year + 1 -> (input.name, input.url.format(year, year + 1))
          }
      }

    val resultfinal = result1.flatMap(
      x =>
        try {
          LeagueStandingSeq(x._2._2, x._2._1, x._1)
        } catch {
          case _: Throwable =>
            // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
            logger.warn(s"Can't parse season ${x._1} from ${x._2._2}")
            // c.printStackTrace
            Seq.empty
        }

    )

      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
    resultfinal.repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("res")


    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?

    // ** Fournit un support extensible pour les codages par colonne (par exemple, delta, durée d'exécution, etc.)
    // ** Fournir l'extensibilité de stocker plusieurs types de données dans les données de colonne (par exemple, les index, les filtres bloom, les statistiques)

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?

    // ** Mise à part les fonctionalités des RDDs nous pouvons aussi utilisé les requêttes SQL et les DSL relatives aux spark Data Frame API
    // ** la Structure de DS nous permet de détecter les erreurs à la compilation.
  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml

    import java.nio.charset.StandardCharsets
    import java.nio.file.Files
    import java.nio.file.Paths
    val yamlSource = new String(Files.readAllBytes(Paths.get("src/main/resources/leagues.yaml")), StandardCharsets.UTF_8)

    mapper.readValue(yamlSource, classOf[Array[LeagueInput]]).toSeq
  }

  private def LeagueStandingSeq(url: String, league: String, season: Int): Seq[LeagueStanding] = {
    var initseq: collection.immutable.Seq[LeagueStanding] = collection.immutable.Seq.empty[LeagueStanding]
    val doc = Jsoup.connect(url).get
    val extractclass1 = doc.getElementsByClass("wikitable gauche")
    val extractclass2 = doc.getElementsByClass("wikitable")

    if (extractclass1.size() != 0) {
      val tr = ChoixTable(extractclass1)


      for (i <- 1 until tr.size()) {
        val position = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(0).text().trim).toSeq.head.toInt
        val team = tr.get(i).getElementsByTag("td").get(1).getElementsByTag("a").get(0).text().trim
        val points = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(2).text().trim).toSeq.head.toInt
        val played = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(3).text().trim).toSeq.head.toInt
        val won = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(4).text().trim).toSeq.head.toInt
        val drawn = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(5).text().trim).toSeq.head.toInt
        val lost = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(6).text().trim).toSeq.head.toInt
        val goalsFor = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(7).text().trim).toSeq.head.toInt
        val goalsAgainst = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(8).text().trim).toSeq.head.toInt
        val goalsDifference = (tr.get(i).getElementsByTag("td").get(9).text().trim).toInt


        val extending = LeagueStanding(league,
          season,
          position,
          team,
          points,
          played,
          won,
          drawn,
          lost,
          goalsFor,
          goalsAgainst,
          goalsDifference)
        initseq = initseq :+ extending
      }
    }

    else {

      val tr = ChoixTable(extractclass2)

      for (i <- 1 until tr.size()) {
        val position = ("""\d+""".r findAllIn (tr.get(i).getElementsByTag("td").get(0).text().trim)).toSeq.head.toInt

        val team = tr.get(i).getElementsByTag("td").get(1).getElementsByTag("a").get(0).text().trim
        //parser le team comme  Grenoble Foot 38 en 2010 dans ligue 1 et juventus FC
        val points = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(2).text().trim).toSeq.head.toInt
        val played = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(3).text().trim).toSeq.head.toInt
        val won = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(4).text().trim).toSeq.head.toInt
        val drawn = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(5).text().trim).toSeq.head.toInt
        val lost = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(6).text().trim).toSeq.head.toInt
        val goalsFor = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(7).text().trim).toSeq.head.toInt
        val goalsAgainst = ("""\d+""".r findAllIn tr.get(i).getElementsByTag("td").get(8).text().trim).toSeq.head.toInt
        val goalsDifference = (tr.get(i).getElementsByTag("td").get(9).text().trim).toInt


        val extending = LeagueStanding(league,
          season,
          position,
          team,
          points,
          played,
          won,
          drawn,
          lost,
          goalsFor,
          goalsAgainst,
          goalsDifference)

        initseq = initseq :+ extending
      }

    }


    initseq
  }

  def ChoixTable(extractclasstest: Elements): Elements = {

    var rbody: Elements = null
    for (i <- 0 until extractclasstest.size()) {
      val tbody = extractclasstest.get(i).getElementsByTag("tbody").get(0)
      if (tbody.getElementsByTag("tr").get(0).getElementsByTag("th").size() == 10) {
        rbody = tbody.getElementsByTag("tr")


      }
    }
    rbody
  }
}

  // TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
  case class LeagueInput(@JsonProperty("name") name: String,
                         @JsonProperty("url") url: String)

  case class LeagueStanding(league: String,
                            season: Int,
                            position: Int,
                            team: String,
                            points: Int,
                            played: Int,
                            won: Int,
                            drawn: Int,
                            lost: Int,
                            goalsFor: Int,
                            goalsAgainst: Int,
                            goalsDifference: Int)


