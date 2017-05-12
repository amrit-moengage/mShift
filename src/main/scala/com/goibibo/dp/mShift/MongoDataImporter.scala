package com.goibibo.dp.mShift

import com.mongodb.hadoop.MongoInputFormat
import com.mongodb.hadoop.splitter.{ MongoPaginatingSplitter,MongoSplitter }
import java.util.Map
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bson.BSONObject
import org.apache.spark.sql._
import scala.collection.convert.wrapAsScala._
import org.slf4j.{Logger, LoggerFactory}
import org.python.util.PythonInterpreter
import org.python.core.{PyObject, PyString}
import org.json4s.jackson.Serialization

/*
spark-submit --class com.goibibo.dp.mShift.main --packages "org.apache.hadoop:hadoop-aws:2.7.2,com.amazonaws:aws-java-sdk:1.7.4,org.mongodb.mongo-hadoop:mongo-hadoop-core:2.0.1,com.databricks:spark-redshift_2.10:1.1.0,org.python:jython-standalone:2.7.0" --files "/tmp/udf.py" --num-executors 2 --jars "/tmp/RedshiftJDBC4-1.1.17.1017.jar" mongodb_redshift_export_2.10-0.1.jar gnsdevice.json
*/

object PythonToJavaConverter {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    
    def createUdfMap(sc:SparkContext, udfNames: Array[String]): scala.collection.immutable.Map[String, org.python.core.PyObject] = {
        logger.info("Udf list - {}", udfNames)

        val filesArgs = sc.getConf.getAll.toMap.get("spark.yarn.dist.files")

        filesArgs match {
            case Some(fileName) => {
                val pyFiles = fileName.split(",").filter(_.contains(".py"))
                if (pyFiles.length == 0) {
                    logger.warn("No python udf file found")
                    return scala.collection.immutable.Map[String, org.python.core.PyObject]()
                }

                logger.info("Files in args - {}", fileName)
                val udfFile = fileName.split("/").last
                logger.info("Loading Udf from py file - {}", udfFile)
                val udfData: String = SchemaReader.readFile(udfFile)
                val interpreter = new PythonInterpreter()
                interpreter.exec(udfData)
                val udfMap = udfNames.map(udfName =>(udfName, interpreter.get(udfName))).toMap
                logger.info("udfMap - {}", udfMap)
                udfMap
            }
            case None => scala.collection.immutable.Map[String, org.python.core.PyObject]() 
        }
    }   
}

object MongoDataImporter {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val formats = org.json4s.DefaultFormats

    def getFieldfromDoc(doc:Map[Object,Object], fieldName:String, udf: Option[String])(implicit jsonConfigData: DataMapping, udfMap: scala.collection.immutable.Map[String, org.python.core.PyObject] ): Any = {
        udf match {
            case Some(udfFunction) => {
                val udfObj = udfMap.get(udfFunction).get
                val resultPy = udfObj.__call__(new PyString(Serialization.write(doc)))
                resultPy.__tojava__(classOf[String]).asInstanceOf[String] 
                }
            case None => {
                if (doc.containsKey(fieldName)) return doc.get(fieldName)
                val index = fieldName.indexOf('.')
                val fieldValue = if (index != -1) {
                    val nestedColumns = fieldName.splitAt(index) 
                    val nestedObj = doc.get(nestedColumns._1)
                    if(Option(nestedObj:Object).isDefined){ 
                        return getFieldfromDoc( nestedObj.asInstanceOf[Map[Object,Object]], nestedColumns._2.tail, udf)
                    } else {
                        logger.info("Couldn't find object for {}", fieldName)
                        logger.debug("Object data is {}", doc)
                    }
                } else {
                    logger.warn("Wrong field name {}",fieldName)
                    null
                }
                fieldValue
            }
        }
    }

    def mapParser(sc: SparkContext, docs: Iterator[Map[Object,Object]])(implicit jsonConfigData: DataMapping) : Iterator[Row] = {

        implicit val udfMap = PythonToJavaConverter.createUdfMap(sc, getUdfList)
        val parsedDocs = docs.map( doc => Row.fromSeq( jsonConfigData.columns.map(column => getFieldfromDoc(doc, column.columnName, column.udf)))).toSeq
        parsedDocs.iterator

    }

    def parser(doc: Map[Object,Object])(implicit jsonConfigData: DataMapping, udfMap: scala.collection.immutable.Map[String, org.python.core.PyObject]) : Row = {
        Row.fromSeq( jsonConfigData.columns.map(column => getFieldfromDoc(doc, column.columnName, column.udf))  )
    }

    def getMongoConfig(dataMapping:DataMapping): Configuration = {
        val mongoConfig = new Configuration()
        logger.info("mongo.input.query = {}", dataMapping.mongoFilterQuery)
        mongoConfig.set("mongo.input.query", dataMapping.mongoFilterQuery)

        logger.info("mongo.input.uri = {}", dataMapping.mongoUrl)
        mongoConfig.set("mongo.input.uri", dataMapping.mongoUrl)

        logger.info("mongo.input.splits.min_docs = {}", Settings.documentsPerSplit)
        mongoConfig.setInt("mongo.input.splits.min_docs", Settings.documentsPerSplit)

        mongoConfig.setClass("mongo.splitter.class", classOf[MongoPaginatingSplitter], classOf[MongoSplitter])
        mongoConfig.setBoolean("mongo.input.notimeout", true)
        mongoConfig.setBoolean("mongo.input.split.use_range_queries", true)
        return mongoConfig
    }

    def getMongoRdd(sc:SparkContext, dataMapping:DataMapping) = {
        sc.newAPIHadoopRDD(
                    getMongoConfig(dataMapping),
                    classOf[MongoInputFormat],  // Fileformat type
                    classOf[Object],            // Key type
                    classOf[BSONObject]         // Value type
                    )
    }

    def getUdfList(implicit dataMapping:DataMapping): Array[String] = dataMapping.columns.filter(_.udf.isDefined).map(_.udf.get)

    def loadData(sc:SparkContext, dataMapping:DataMapping): RDD[Row] = {

        implicit val columnSourceNames: Seq[String] = SchemaReader.getColumnSourceList(dataMapping)
        implicit val jsonConfigData: DataMapping = dataMapping
        implicit val tmpUdfMap = scala.collection.immutable.Map[String, org.python.core.PyObject]()

        logger.info("getUdfList  = {}", getUdfList)
        logger.info("columnSourceNames = {}", columnSourceNames)

        dataMapping.pyUdf match {
            case Some(pyUdfFile) => {
                logger.info("pyUdf File found = {}",pyUdfFile)        
                getMongoRdd(sc,dataMapping).map(_._2.asInstanceOf[Map[Object,Object]]).mapPartitions( mapParser(sc, _) )
                }
            case None => getMongoRdd(sc,dataMapping).map(_._2.asInstanceOf[Map[Object,Object]])
                    .map(d => parser(d))
        }
    }
}
