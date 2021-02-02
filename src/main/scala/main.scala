import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryExpression, BinaryOperator, Expression, Or, SortOrder}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, ReturnAnswer, Sort, SubqueryAlias}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Main {

    val sparkSession = SparkSession.builder
        .appName("Taster")
        .master("local[*]")
        .getOrCreate();
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
    val tableCounter = new mutable.HashMap[String, Int]()
    var mapRDDScanRowCNT: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
    var subTreeCount: mutable.HashMap[LogicalPlan, Long] = new mutable.HashMap[LogicalPlan, Long]()
    var numberOfExecutedSubQuery = 0
    val threshold = 1000
    var numberOfRemovedSynopses = 0;
    val DATA_DIR = "/Users/falker/Documents/Projects/skyserver/data/";
    val BENCH_DIR = "/Users/falker/Documents/Projects/skyserver/querylogs/";
    val folder: Array[File] = (new File(DATA_DIR)).listFiles.filter(_.isDirectory)


    def main(args: Array[String]): Unit = {

        val dataframes = loadTables()
        val queries: List[String] = queryWorkload()
//        val queries: List[String] = List("select count(*) from specobj as s join photoobj as p on s.specobjid = p.specobjid", "select count(*) from specobj as s join photoobj as p on s.specobjid = p.specobjid")

        processQueries(queries)


    }

    def loadTables(): Map[String, DataFrame] = {
        var dataframes = Map[String, DataFrame]()

        for (i <- folder.indices) {
            if (!folder(i).getName.contains(".csv") && !folder.exists(_.getName == folder(i).getName + ".csv")) {
                println("Reading: " + folder(i).getName + ".csv")
                val loaded = sparkSession.sqlContext
                    .read.format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ",")
                    .option("nullValue", "null")
                    .load(DATA_DIR + "/" + folder(i).getName + "/" + folder(i).getName + ".csv")
                //        dataframes(i) = loaded
                dataframes += (folder(i).getName.capitalize -> loaded)
                sparkSession.sqlContext.createDataFrame(loaded.rdd, loaded.schema).createOrReplaceTempView(folder(i).getName.capitalize)
            }
        }
        //    sparkSession.table("SpecObj").printSchema()
        //    dataframes(1).printSchema()
        sparkSession.catalog.listTables()
        dataframes.keys.foreach((k) =>
            println(k, sparkSession.table(k).count())
        )
        dataframes
    }

    def queryWorkload(): List[String] = {
        var temp: ListBuffer[String] = ListBuffer();
        var src = Source.fromFile(BENCH_DIR + "2013.csv").getLines
        src.take(1).next
        var counter = 0
        val queryLog = sparkSession.sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
            .option("inferSchema", "true").option("delimiter", ";").option("nullValue", "null").load(BENCH_DIR + "2013.csv");
        // queryLog.filter("rows>0").sort(col("clientIP"),col("seq").asc).select("statement").show()
        for (l <- src) {
            if (!l.contains("round") && !l.contains("petror50") && !l.contains("d.branch_name") && !l.contains("p.redshift") && !l.contains("s.zt") && !l.contains("avg(s.redshift)") && !l.contains("avg(redshift)") && !l.contains("count(8) ra,dec") && !l.contains("objid,ra,dec,dered_u,dered_g,dered_r,dered_i,dered_z") && !l.contains("nik") && !l.contains("specobj,count(*)fr") && !l.contains("count(*) from specobj,count(*)") && !l.contains("specobj,photoobj") && !l.contains("legacyprimary") && !l.contains("galspecextra") && !l.contains("petrorad") && !l.contains("petrorad_r_50") && !l.contains("petrorad_50_r") && !l.contains("petrorad50_r") && !l.contains("radius") && !l.contains("petr50_r") && !l.contains("petror50_r") && !l.contains("g,err_g,r,i,z,g-r") && !l.contains("h83side") && !l.contains("zoo2mainphotoz") && !l.contains("apogeestar") && !l.contains("zoo2mainspecz") && !l.contains("floor(") && !l.contains("round(") && !l.contains("sysdatabases") && !l.contains("information_schema") && !l.contains("logintest") && !l.contains("phototag") && !l.contains("...") && !l.contains("description") && !l.contains("select     count(*),specobjid,survey,mjd,ra,dec,z,psfmag_u,class  from specphotoall  where class='qso' and z>=0  order by z,ra asc") && !l.contains("select type, count(type) from photoobj") && !l.contains("class like type") && !l.contains("tipo") && !l.contains("select count(*) from specobj where ra between 159 and 164 and u > 18 and u-g>2.2") && !l.contains("phototype") && !l.contains("select count(p.ra) from photoobj as p inner join photoobj as gno on p.objid = gno.objid where p.type = 6 and r > 15. and r < 19.") && !l.contains("photozrf") && !l.contains("b.zconf") && !l.contains("specobj where dec>") && !l.contains("+ﬂoor(") && !l.contains("xoriginalid") && !l.contains("select subclass, count(subclass)") && !l.contains("sn_") && !l.contains("crossoriginalid") && !l.contains("modelmag") && !l.contains("select  count(*)  from specobjall s  where s.ra >=") && !l.contains("gz1_agn_mel6002") && !l.contains("eclass") && !l.contains("group by (htmid") && !l.contains("select count(*) from photoobjall") && !l.contains("photoprofile") && !l.contains("peak/snr") && !l.contains("twomass") && !l.contains("masses") && !l.contains(" & ") && !l.contains("count(*)  p.objid") && !l.contains("count(*) p.objid") && !l.contains("count(*), p.objid") && !l.contains("count(*), where") && !l.contains("count(*),where") && !l.contains("count(*)   where") && !l.contains("count(*)  where") && !l.contains("count(*) where") && !l.contains("st.objid") && !l.contains("stellarmassstarformingport") && !l.contains("thingindex") && !l.contains("0x001") && !l.contains("dr9") && !l.contains("fphotoflags") && !l.contains("avg(dec), from") && !l.contains("emissionlinesport") && !l.contains("stellarmasspassiveport") && !l.contains("s.count(z)") && !l.contains("nnisinside") && !l.contains("petromag_u") && !l.contains("insert") && !l.contains("boss_target1") && !l.contains(" photoobj mode = 1") && !l.contains("and count(z)") && !l.contains("gal.extinction_u") && !l.contains("spectroflux_r") && !l.contains("platex") && !l.contains("0x000000000000ffff") && !l.contains("neighbors") && !l.contains("specline") && !l.contains("specclass")) {
                try {
                    if (l.split(";")(8).toLong > 0) {
                        if (l.split(';')(9).size > 30)
                            temp.+=(l.split(';')(9).replace("count(p)", "count(p.type)").replace(" and 30  s.bes", " and 30  and s.bes").replace(" and 25  s.bes", " and 25  and s.bes").replace(" and       and ", " and ").replace(" and      and ", " and ").replace(" and     and ", " and ").replace(" and    and ", " and ").replace(" and   and ", " and ").replace(" and  and ", " and ").replace(" and and ", " and ").replace("from  as photoobjall", "from   photoobjall").replace("\"title\"", " ").replace("p.type \"type\"", "p.type ").replace("\"kirks typo\"", "  ").replace("\"count\"", "  ").replace("\"avg\"", "  ").replace("\"average\"", "  ").replace("\"redshift avg\" ", "  ").replace("\"average redshift from spec table\"", "  ").replace(" eq ", " = ").replace("and avg(petrorad_r) < 18", " ").replace("gobjid", "objid").replace("photoobj 40.97, 13)", "photoobj").replace("sspparams", "sppparams").replace("0.6*round(z/0.6", "z").replace("0.6*round((z+0.3)/0.6 +0.3", "z").replace("0.6*round((z+0.3)/0.6 +0.5", "z").replace("0.6*round((z+0.3)/0.6 -0.5", "z").replace("0.6*round((z+0.3)/0.6 -0.3", "z").replace("0.6*round(z/0.6", "z").replace("-0.3+0.6*round((z+0.3)/0.6", "z").replace("photoobj 10)", "photoobj ").replace("z>=0)", "z>=0").replace("s.zwarning = 0  p.ra between", "s.zwarning = 0 and p.ra between ").replace("order by galaxy", " ").replace("spectrotype", " ").replace("5 and class=galaxy", "5 ").replace(".3  join specobj as s on spectrotype", ".3 ").replace(".3  join specobj as s on s.bestobjid = p.objid", ".3 ").replace(".3  join specobj as s on galspecinfo", ".3 ").replace("grop by galaxy", " ").replace("2 group by galaxy", "2 ").replace("2 and group by type", "2 ").replace("40.2 class like galaxy", "40.2 ").replace("p.g > 2.2", "g > 2.2").replace("betwen", "between").replace("count(type*)", "count(*)").replace("type count(*)", "type, count(*)").replace("type=\"tipo\"", "and type=\"tipo\"").replace("group by type join photoobj as s on s. photo type=name", "group by type").replace("group by type join photoobj as p on p.photo type=name", "group by type").replace("group by type=name", "group by type").replace("group by type join photoobj as s on s. photo type", "group by type").replace("group by type join type as on typen = typen", "group by type").replace("group by type join type on typen = typen", "group by type").replace("group by type = typen", "group by type").replace("group by type join type on typen", "group by type").replace("group by type join photoobj as s on s. photo type=name", "group by type").replace("group by type join type on type-n", "group by type").replace("group by type join type on type.n", "group by type").replace("group by type join photoobj as p on p.photo type=name", "group by type").replace("group by type join photoobj as p on p.type=value", "group by type").replace("u-g 2.2", "u-g > 2.2").replace("group by type join phototype p.fphototypen=p.fphototype", "group by type").replace("group by type join type on fphototype = fphototype", "group by type").replace("group by type join photoobj as p on p.type=value", "group by type").replace("group by type join type on value = value", "group by type").replace("group by type join type on name = name", "group by type").replace("group by type where  join type on name = name", "group by type").replace("group by type join phototype", "group by type").replace("group by type where join type on name = name", "group by type").replace("ra,dec select count(*)", "ra,dec").replace("galaxies", "specobjid").replace("onjid", "objid").replace("avg(aterm_r), avg(kterm_r), avg(airmass_r), avg(aterm_r + kterm_r*airmass_r), gain_r", "avg(aterm_r), avg(kterm_r), avg(airmass_r), avg(aterm_r + kterm_r*airmass_r), avg(gain_r)").replace("select   from field", "field").replace("select   * from field", "field").replace("a.fromvalue, a.tovalue, count(b.z) 'count'", "a.fromvalue, a.tovalue, count(b.z)").replace("bestdr9..", "").replaceAll("\\s{2,}", " ").trim())
                    } else
                        counter = counter + 1
                }
                catch {
                    case e: Exception =>

                }
            }
        }
        // println("number of queries: " + temp.size)
        temp.toList
    }

    def processQueries(queries: List[String]) = {
        var outString = ""
        println(queries.length)
        var count = 0
        val start = 0
        val testSize = 10
        for (i <- start until start + queries.length) {
            val query = queries(i)

            println("query")
            println(query)
            //            val (query_code, confidence, error, dataProfileTable, quantileCol, quantilePart, binningCol,
            //            binningPart, binningStart, binningEnd, table, tempQuery) = tokenizeQuery(query)


            try {
                println("rawPlan")
                val rawPlan = sparkSession.sqlContext.sql(query).queryExecution.analyzed
                println(rawPlan)
                println("logicalPlan")
                val logicalPlan = sparkSession.sessionState.optimizer.execute(rawPlan)
                println(logicalPlan)

                println("Table Joins")
                getSubTrees(logicalPlan)
                count += 1
            }
            catch {
                case e: Exception =>
            }


            //            val subQueries = getAggSubQueries(sparkSession.sqlContext.sql(query_code).queryExecution.analyzed)
            //            for (subQuery <- subQueries) {
            //                println("subQuery")
            //                println(subQuery)
            //                val rawPlans = enumerateRawPlanWithJoin(subQuery)
            //                println("rawPlans")
            //                println(rawPlans)
            //                println("logicalPlans")
            //                val logicalPlans = rawPlans.map(x => sparkSession.sessionState.optimizer.execute(x))
            //                println(logicalPlans)
            //                getTableJoins(logicalPlans)
            //
            //
            ////                val physicalPlans = logicalPlans.flatMap(x => sparkSession.sessionState.planner.plan(ReturnAnswer(x)))
            ////                println(physicalPlans)
            //
            //            }
            //        }
        }
        println("Processed total queries: " + count)
        println("Total unique subtrees: " + subTreeCount.size)
        println("Top 10 frequent subtrees")
        println(subTreeCount.toSeq.sortWith(_._2 > _._2).take(10))
        println(subTreeCount.values.toList.sorted(Ordering.Long.reverse).take(5000))
    }

    def getSubTrees(lp: LogicalPlan): Unit = {
        subTreeCount(lp) = subTreeCount.getOrElse[Long](lp, 0) + 1
        if (lp.children.nonEmpty) {
            lp.children.foreach(x => {
                println("Found subtree")
                println(x)
                getSubTrees(x)
            })
        }
    }

    def getAggSubQueries(lp: LogicalPlan): Seq[LogicalPlan] = lp match {
        case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
            Seq(a) ++ getAggSubQueries(child)
        case a =>
            a.children.flatMap(child => getAggSubQueries(child))
    }


    def tokenizeQuery(query: String) = {
        var confidence = 0.0
        var error = 0.0
        val tokens = query.split(" ")
        var dataProfileTable = ""
        var quantileCol = ""
        var quantilePart = 0
        var binningCol = ""
        var binningPart = 0
        var binningStart = 0.0
        var binningEnd = 0.0
        var table = ""
        var tempQuery = ""
        for (i <- 0 to tokens.size - 1)
            if (tokens(i).equalsIgnoreCase("confidence")) {
                confidence = tokens(i + 1).toInt
                tokens(i) = ""
                tokens(i + 1) = ""
            } else if (tokens(i).equalsIgnoreCase("error")) {
                error = tokens(i + 1).toInt
                tokens(i) = ""
                tokens(i + 1) = ""
            }
            else if (tokens(i).equalsIgnoreCase("dataProfile"))
                dataProfileTable = tokens(i + 1)
            else if (tokens(i).compareToIgnoreCase("quantile(") == 0 || tokens(i).compareToIgnoreCase("quantile") == 0 || tokens(i).contains("quantile(")) {
                val att = query.substring(query.indexOf("(") + 1, query.indexOf(")")).split(",")
                quantileCol = att(0)
                quantilePart = att(1).toInt
                tempQuery = "select " + quantileCol + " " + tokens.slice(tokens.indexOf("from"), tokens.indexOf("confidence")).mkString(" ")
            }
            else if (tokens(i).compareToIgnoreCase("binning(") == 0 || tokens(i).compareToIgnoreCase("binning") == 0 || tokens(i).contains("binning(")) {
                val att = query.substring(query.indexOf("(") + 1, query.indexOf(")")).split(",")
                binningCol = att(0)
                binningStart = att(1).toDouble
                binningEnd = att(2).toDouble
                binningPart = att(3).toInt
            }
            else if (tokens(i).equalsIgnoreCase("from"))
                table = tokens(i + 1)
        if (confidence < 0 || confidence > 100 || error < 0 || error > 100)
            throw new Exception("Invalid error and confidence")
        confidence /= 100
        error /= 100
        (tokens.mkString(" "), confidence, error, dataProfileTable, quantileCol, quantilePart.toInt, binningCol, binningPart.toInt
            , binningStart, binningEnd, table, tempQuery)
    }

    def getJoinConditions(exp: Expression): Seq[BinaryExpression] = exp match {
        case a@And(left, right) =>
            return (getJoinConditions(left) ++ getJoinConditions(right))
        case o@Or(left, right) =>
            return (getJoinConditions(left) ++ getJoinConditions(right))
        case b@BinaryOperator(left, right) =>
            if (left.find(_.isInstanceOf[AttributeReference]).isDefined && right.find(_.isInstanceOf[AttributeReference]).isDefined)
                return Seq(b)
            Seq()
        case a =>
            Seq()
    }


    def enumerateRawPlanWithJoin(rawPlan: LogicalPlan): Seq[LogicalPlan] = {
        return Seq(rawPlan)
        var rootTemp: ListBuffer[LogicalPlan] = new ListBuffer[LogicalPlan]
        val subQueries = new ListBuffer[SubqueryAlias]()
        val queue = new mutable.Queue[LogicalPlan]()
        var joinConditions: ListBuffer[BinaryExpression] = new ListBuffer[BinaryExpression]()
        queue.enqueue(rawPlan)
        while (!queue.isEmpty) {
            val t = queue.dequeue()
            t match {
                case SubqueryAlias(name, child) =>
                    subQueries.+=(t.asInstanceOf[SubqueryAlias])
                case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
                    queue.enqueue(child)
                    rootTemp.+=(Aggregate(groupingExpressions, aggregateExpressions, child))
                case Filter(conditions, child) =>
                    joinConditions.++=(getJoinConditions(conditions))
                    queue.enqueue(child)
                    rootTemp.+=(Filter(conditions, rawPlan.children(0).children(0)))
                case org.apache.spark.sql.catalyst.plans.logical.Join(
                left: LogicalPlan, right: LogicalPlan, joinType: JoinType,
                condition: Option[Expression]) =>
                    queue.enqueue(left)
                    queue.enqueue(right)
                    if (condition.isDefined)
                        joinConditions.+=(condition.get.asInstanceOf[BinaryExpression])
                case Project(p, c) =>
                    queue.enqueue(c)
                    rootTemp.+=(Project(p, rawPlan.children(0).children(0)))
                case Sort(order: Seq[SortOrder], global: Boolean, child: LogicalPlan) =>
                    queue.enqueue(child)
                    rootTemp += Sort(order, global, rawPlan.children(0).children(0))
                case u@UnresolvedRelation(table) =>
                    subQueries.+=(SubqueryAlias(AliasIdentifier(table.table), u))

                case _ =>
                    throw new Exception("new logical raw node")
            }
        }
        var allPossibleJoinOrders = new ListBuffer[Seq[LogicalPlan]]
        allPossibleJoinOrders.+=(Seq())
        allPossibleJoinOrders.+=(subQueries)
        for (i <- 2 to subQueries.size) {
            val temp = new ListBuffer[Join]
            for (j <- (1 to i - 1))
                if (j == i - j) {
                    for (l <- 0 to allPossibleJoinOrders(j).size - 1)
                        for (r <- l + 1 to allPossibleJoinOrders(j).size - 1)
                            if (!hasTheSameSubquery(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r))
                                && isJoinable(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), joinConditions))
                                temp.+=:(Join(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), org.apache.spark.sql.catalyst.plans.Inner, Some(chooseConditionFor(allPossibleJoinOrders(j)(l), allPossibleJoinOrders(j)(r), joinConditions))))
                } else if (j < i - j) {
                    for (left <- allPossibleJoinOrders(j))
                        for (right <- allPossibleJoinOrders(i - j))
                            if (!hasTheSameSubquery(left, right) && isJoinable(left, right, joinConditions)) {
                                val jj = (Join(left, right, org.apache.spark.sql.catalyst.plans.Inner, Some(chooseConditionFor(left, right, joinConditions))))
                                //if(!isCartesianProduct(jj))
                                temp.+=:(jj)
                            }
                }
            allPossibleJoinOrders.+=(temp)
        }
        var plans = new ListBuffer[LogicalPlan]()
        for (j <- 0 to allPossibleJoinOrders(subQueries.size).size - 1) {
            plans.+=(allPossibleJoinOrders(subQueries.size)(j))
            for (i <- rootTemp.size - 1 to 0 by -1) {
                rootTemp(i) match {
                    case Aggregate(groupingExpressions, aggregateExpressions, child) =>
                        plans(j) = Aggregate(groupingExpressions, aggregateExpressions, plans(j))
                    case Filter(condition, child) =>
                        plans(j) = Filter(condition, plans(j))
                    case Project(p, c) =>
                        plans(j) = Project(p, plans(j))
                    case Sort(o, g, c) =>
                        plans(j) = Sort(o, g, plans(j))
                    case _ =>
                        throw new Exception("new logical node")
                }
            }
        }
        plans
    }

    def isJoinable(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Boolean = {
        for (i <- 0 to joinConditions.length - 1) {
            val lJoinkey = Set(joinConditions(i).left.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
            val rJoinkey = Set(joinConditions(i).right.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
            if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet))
                || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet)))
                return true
        }
        false
    }

    def hasTheSameSubquery(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
        val queue = new mutable.Queue[LogicalPlan]()
        val subquery = new ListBuffer[AliasIdentifier]
        queue.enqueue(plan2)
        while (!queue.isEmpty) {
            val t = queue.dequeue()
            t match {
                case y@SubqueryAlias(name, child) =>
                    subquery.+=(name)
                case _ =>
                    t.children.foreach(x => queue.enqueue(x))
            }
        }
        hasSubquery(plan1, subquery)
    }

    def hasSubquery(plan: LogicalPlan, query: Seq[AliasIdentifier]): Boolean = {
        plan match {
            case SubqueryAlias(name: AliasIdentifier, child: LogicalPlan) =>
                if (query.contains(name))
                    return true
            case _ =>
                plan.children.foreach(x => if (hasSubquery(x, query) == true) return true)
        }
        false
    }

    def chooseConditionFor(lp1: LogicalPlan, lp2: LogicalPlan, joinConditions: ListBuffer[BinaryExpression]): Expression = {
        for (i <- 0 to joinConditions.length - 1) {
            val lJoinkey = Set(joinConditions(i).left.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
            val rJoinkey = Set(joinConditions(i).right.find(_.isInstanceOf[AttributeReference]).get.toString().toLowerCase)
            if ((lJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet))
                || (lJoinkey.subsetOf(lp2.output.map(_.toAttribute.toString().toLowerCase).toSet) && rJoinkey.subsetOf(lp1.output.map(_.toAttribute.toString().toLowerCase).toSet)))
                return joinConditions(i).asInstanceOf[Expression]
        }
        throw new Exception("Cannot find any condition to join the two tables")
    }


}