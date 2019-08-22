package my.test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ArrayBuffer

case class Table(name: String, alias: String, schema: Schema) {
  def allColumnsSimplified: String = alias+".*"

  def allColumns: String = {
    val ab = new ArrayBuffer[String]()
    schema.columns.foreach(c => ab.append(alias+"."+c.name+" AS "+c.name))
    ab.mkString(",")
  }

  def allColumnsWithException(exceptColumns: List[String]): String = {
    val ab = new ArrayBuffer[String]()
    schema.columns.foreach(c => if (!exceptColumns.contains(c.name)) ab.append(alias+"."+c.name+" AS "+c.name))
    ab.mkString(",")
  }

  def allColumnsWithConstReplacement(replacement: Map[String, String]): String = {
    val ab = new ArrayBuffer[String]()
    schema.columns.foreach(c => {
      if (replacement.contains(c.name)) {
        ab.append("'"+replacement(c.name)+"'"+" AS "+c.name)
      } else {
        ab.append(alias+"."+c.name+" AS "+c.name)
      }
    })
    ab.mkString(",")
  }

  def allColumnsWithReplacement(replacement: MutableMap[String, String]): String = {
    val ab = new ArrayBuffer[String]()
    schema.columns.foreach(c => {
      if (replacement.contains(c.name)) {
        ab.append(replacement(c.name)+" AS "+c.name)
      } else {
        ab.append(alias+"."+c.name+" AS "+c.name)
      }
    })
    ab.mkString(",")
  }

  def pkAndAlias: String = {
    val c = schema.getPk
    alias+"."+c.name+" AS "+c.name
  }

  def pk: String = {
    val c = schema.getPk
    alias+"."+c.name
  }

  def scd1ColumnsAlias: String = {
    val ab = new ArrayBuffer[String]()
    schema.getSCD1Columns.foreach(c => ab.append(alias+"."+c.name+" AS "+c.name))
    ab.mkString(",")
  }

  def scd2ColumnsAlias: String = {
    val ab = new ArrayBuffer[String]()
    schema.getSCD2Columns.foreach(c => ab.append(alias+"."+c.name+" AS "+c.name))
    ab.mkString(",")
  }

  def nonSCDColumnsAlias: String = {
    val ab = new ArrayBuffer[String]()
    schema.getNonSCDColumns.foreach(c => ab.append(alias+"."+c.name+" AS "+c.name))
    ab.mkString(",")
  }
}

case class Schema(columns: List[Column]) {
  def getPk: Column = {
    var pk: Column = null
    columns.foreach(c => if (c.isPk) pk = c)
    pk
  }

  def getSCD1Columns: List[Column] = {
    val ab = ArrayBuffer[Column]()
    columns.foreach(c => if (c.scdType==1) ab.append(c))
    ab.toList
  }

  def getSCD2Columns: List[Column] = {
    val ab = ArrayBuffer[Column]()
    columns.foreach(c => if (c.scdType==2) ab.append(c))
    ab.toList
  }

  def getNonSCDColumns: List[Column] = {
    val ab = ArrayBuffer[Column]()
    columns.foreach(c => if (c.scdType==(-1)) ab.append(c))
    ab.toList
  }
}

case class Column(name: String, dataType: String, scdType: Int = -1, isPk: Boolean = false)


object AutoSCD {
  def getSparkSession(master: String, appName: String) = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    SparkSession.builder()
        .master(master)
        .appName(appName)
        .config("spark.testing.memory", "2147480000")
        .enableHiveSupport()
        .getOrCreate()
  }

  def getDate() = {
    val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE, 0)
    ymdFormat.format(day.getTime)
  }

  def getPreDate() = {
    val ymdFormat = new SimpleDateFormat("yyyy-MM-dd")
    val day:Calendar = Calendar.getInstance()
    day.add(Calendar.DATE, -1)
    ymdFormat.format(day.getTime)
  }
  def pkEqualCondition(t1: Table, t2: Table): String = {
    val c1 = t1.schema.getPk
    val c2 = t2.schema.getPk
    t1.alias+"."+c1.name+"="+t2.alias+"."+c2.name
  }

  def scd1EqualCondition(t1: Table, t2: Table): String = {
    val ab = new ArrayBuffer[String]()
    (t1.schema.getSCD1Columns, t2.schema.getSCD1Columns).zipped.foreach((c1,c2) => {
      ab.append(t1.alias+"."+c1.name+"="+t2.alias+"."+c2.name)
    })
    ab.mkString(" AND ")
  }

  def scd2EqualCondition(t1: Table, t2: Table): String = {
    val ab = new ArrayBuffer[String]()
    (t1.schema.getSCD2Columns, t2.schema.getSCD2Columns).zipped.foreach((c1,c2) => {
      ab.append(t1.alias+"."+c1.name+"="+t2.alias+"."+c2.name)
    })
    ab.mkString(" AND ")
  }

  def scd1NotEqualCondition(t1: Table, t2: Table): String = {
    val ab = new ArrayBuffer[String]()
    (t1.schema.getSCD1Columns, t2.schema.getSCD1Columns).zipped.foreach((c1,c2) => {
      ab.append(t1.alias+"."+c1.name+"<>"+t2.alias+"."+c2.name)
    })
    ab.mkString(" OR ")
  }

  def scd2NotEqualCondition(t1: Table, t2: Table): String = {
    val ab = new ArrayBuffer[String]()
    (t1.schema.getSCD2Columns, t2.schema.getSCD2Columns).zipped.foreach((c1,c2) => {
      ab.append(t1.alias+"."+c1.name+"<>"+t2.alias+"."+c2.name)
    })
    ab.mkString(" OR ")
  }

  def getFieldReplacement(baseTable: Table, stageTable: Table, replaceType: String): MutableMap[String, String] = {
    val scd1Maps = MutableMap[String, String]()
    val alias = stageTable.alias
    if (replaceType=="scd1") {
      (baseTable.schema.getSCD1Columns, stageTable.schema.getSCD1Columns).zipped.foreach((bc, sc) => {
        scd1Maps += (bc.name -> (alias+"."+sc.name))
      })
    } else if (replaceType=="scd2") {
      (baseTable.schema.getSCD2Columns, stageTable.schema.getSCD2Columns).zipped.foreach((bc, sc) => {
        scd1Maps += (bc.name -> (alias+"."+sc.name))
      })
    }
    scd1Maps
  }

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession("local[2]", "AutoSCD")
    val baseSchema = Schema(List(
      Column("sk", "int"),
      Column("id", "int", isPk = true),
      Column("name", "string", scdType = 2),
      Column("city", "string", scdType = 1),
      Column("st", "string", scdType = 1),
      Column("update_date", "string"),
      Column("scd_version", "int"),
      Column("scd_valid_flag", "string"),
      Column("scd_start_date", "string"),
      Column("scd_end_date", "string")
    ))
    val stageSchema = Schema(List(
      Column("id", "int", isPk = true),
      Column("name", "string", scdType = 2),
      Column("city", "string", scdType = 1),
      Column("st", "string", scdType = 1),
      Column("update_date", "string")
    ))
    val baseTable = Table("base", "b", baseSchema)
    val stageTable = Table("stage", "s", stageSchema)
    val pre_date = getPreDate()
    val max_date = "9999-12-31"
    //    println(baseTable.allColumns)
    //    println(baseTable.allColumnsWithConstReplacement(Map("scd_valid_flag"->"N", "scd_end_date" -> pre_date)))
    //    println(stageTable.scd1Columns)
    //    println(stageTable.pks)
    //    println(pkEqualCondition(baseTable, stageTable))
    //    println(scd1EqualCondition(baseTable, stageTable))
    //    println(scd2EqualCondition(baseTable, stageTable))

    import spark.sql
    import spark.implicits._
    sql("use mydb")
    sql("drop table if exists base_dim")
    sql(
      s"""
         | create table base_dim
         |    (sk int, id int, name string, city string, st string, update_date string,scd_version int,scd_valid_flag string,scd_start_date string, scd_end_date string)
         | stored as orc
                 """.stripMargin)
    sql(
      s"""
         | insert into table base_dim
         | values
         |    (1,1,"zhangsan","us","ca","2019-01-01",1,"Y","2019-01-01","9999-12-31"),
         |    (2,2,"lisi","us","cb","2019-01-01",1,"Y","2019-01-01","9999-12-31"),
         |    (3,3,"wangwu","ca","bb","2019-01-01",1,"Y","2019-01-01","9999-12-31"),
         |    (4,4,"zhaoliu","ca","bc","2019-01-01",1,"Y","2019-01-01","9999-12-31"),
         |    (5,5,"mazi","aa","aa","2019-01-01",1,"Y","2019-01-01","9999-12-31")
                 """.stripMargin)
    sql("drop table if exists stage")
    sql("create table stage (id int, name string, city string, st string, update_date string) stored as orc")
    // 1: scd2 + scd1; 2: expired(snapshot) | unchanged (incrementing mode); 3: unchanged; 4: scd1; 5: scd2; 6: new record
    val dt = getDate()
    sql(
      s"""
         | insert into table stage
         | values
         |    (1,"zhang","u","c",'$dt'),
         |    (3,"wangwu","ca","bb",'$dt'),
         |    (4,"zhaoliu","ac","cb",'$dt'),
         |    (5,"ma","aa","aa",'$dt'),
         |    (6,"laoyang","dd","dd",'$dt')
                 """.stripMargin)

    // START
    // UNCHANGED RECORDS
    val unchangedDF = sql(
      s"""
         |SELECT
         |  ${baseTable.allColumnsSimplified}
         |FROM
         |  base_dim b
         |  JOIN stage s
         |  ON ${pkEqualCondition(baseTable, stageTable)}
         |  AND ${scd1EqualCondition(baseTable, stageTable)}
         |  AND ${scd2EqualCondition(baseTable, stageTable)}
       """.stripMargin)
    val unchangedDF2 = sql(
      s"""
         |SELECT
         |  ${baseTable.allColumnsSimplified}
         |FROM
         |  base_dim b
         |  LEFT JOIN stage s
         |  ON ${pkEqualCondition(baseTable, stageTable)}
         |  WHERE ${stageTable.pk} IS NULL
         |  OR ${scd1EqualCondition(baseTable, stageTable)} AND ${scd2EqualCondition(baseTable, stageTable)}
       """.stripMargin)
    //    unchangedDF.show()
    //    unchangedDF2.show()

    // mark new expired records
    val expireDF = sql(
      s"""
         |SELECT
         |  ${baseTable.allColumnsWithConstReplacement(Map("scd_valid_flag"->"N", "scd_end_date" -> pre_date))}
         |FROM
         |  (SELECT * FROM base_dim WHERE scd_end_date='$max_date') b
         |  LEFT JOIN stage s
         |  ON ${pkEqualCondition(baseTable, stageTable)}
         |  WHERE ${stageTable.pk} IS NULL
         |  OR ${scd2NotEqualCondition(baseTable, stageTable)}
       """.stripMargin
    )
    val expireDF2 = sql(
      s"""
         |SELECT
         |  ${baseTable.allColumnsWithConstReplacement(Map("scd_valid_flag"->"N", "scd_end_date" -> pre_date))}
         |FROM
         |  (SELECT * FROM base_dim WHERE scd_end_date='$max_date') b
         |  LEFT JOIN stage s
         |  ON ${pkEqualCondition(baseTable, stageTable)}
         |  WHERE ${scd2NotEqualCondition(baseTable, stageTable)}
       """.stripMargin)
    //    expireDF.show()
    //    expireDF2.show()

    // new SCD2 records
    val t1 = Table("t1", "t1", baseSchema)
    val scd2NewLineDF = sql(
      s"""
         | SELECT
         |     ROW_NUMBER() OVER (ORDER BY t1.id) + t2.sk_max sk,
         |     ${t1.allColumnsWithException(List("sk"))}
         | FROM
         |    (
         |        SELECT
         |            ${stageTable.allColumnsSimplified},
         |            b.scd_version + 1 scd_version,
         |            "Y" scd_valid_flag,
         |            b.scd_start_date scd_start_date,
         |            '$max_date' scd_end_date
         |        FROM
         |            base_dim b
         |            JOIN
         |                stage s
         |            ON ${pkEqualCondition(baseTable, stageTable)}
         |            AND b.scd_valid_flag = 'Y'
         |            AND ${scd2NotEqualCondition(baseTable, stageTable)}
         |    ) t1
         |    CROSS JOIN (SELECT COALESCE(MAX(sk),0) sk_max FROM base_dim) t2
         |
                 """.stripMargin)
    //    scd2NewLineDF.show()

    // only SCD1 changes

    val replacement = getFieldReplacement(baseTable, stageTable, "scd1")
    val scd1UpdateDF = sql(
      s"""
         | SELECT
         |    ${baseTable.allColumnsWithReplacement(replacement)}
         | FROM
         |    base_dim b
         |    JOIN
         |        stage s
         |    ON ${pkEqualCondition(baseTable, stageTable)}
         |    AND b.scd_valid_flag = 'Y'
         |    AND ${scd2EqualCondition(baseTable, stageTable)}
         |    WHERE ${scd1NotEqualCondition(baseTable, stageTable)}
       """.stripMargin)
    //    scd1UpdateDF.show()

    // brand new records

    val scdDF = unchangedDF2.union(expireDF2).union(scd2NewLineDF).union(scd1UpdateDF)
    scdDF.createOrReplaceTempView("new_base_dim")
    // grand-new records
    val brandNewDF = sql(
      s"""
         | SELECT
         |    ROW_NUMBER() OVER (ORDER BY t1.id) + t2.sk_max sk,
         |    t1.*,
         |    1,
         |    "Y",
         |    '$pre_date',
         |    '$max_date'
         | FROM
         |    (stage s LEFT ANTI JOIN base_dim b ON ${pkEqualCondition(baseTable, stageTable)}) t1
         |    CROSS JOIN (SELECT COALESCE(MAX(sk),0) sk_max FROM new_base_dim) t2
                 """.stripMargin)
    // brandNewDF.show()
    val finalDF = scdDF.union(brandNewDF)
    finalDF.show()

    // stop spark session
    spark.stop()

  }
}
