// Databricks notebook source
// MAGIC %md Ensure Spark2.1 db5 is used to run this notebook

// COMMAND ----------

// MAGIC %md
// MAGIC This notebook, starts a streaming function to process a Kinesis Stream. 
// MAGIC 1. Listens and retrieves messages from the ci_delete kinesis stream
// MAGIC 2. Converts the message to a case class 
// MAGIC 2. Save the data to a S3 location
// MAGIC 
// MAGIC 
// MAGIC The notebook depends on the following libraries and they need to be attached to the cluster:
// MAGIC </br>
// MAGIC location: /data-platform-qa/cisco_import_changes_3.13/libraries
// MAGIC 
// MAGIC </br>
// MAGIC mongo-java-driver-3.2.2.jar
// MAGIC </br>
// MAGIC mongo-spark-connector_2.11-2.0.0.jar
// MAGIC </br>
// MAGIC common-3.10.7.jar
// MAGIC </br>
// MAGIC datalake_deng_2.11-03.13.00.jar
// MAGIC </br>
// MAGIC datalake_analytics_2_11_0_0_0_9033.jar
// MAGIC </br>

// COMMAND ----------

sc.hadoopConfiguration.set("fs.s3a.credentialsType", "AssumeRole")

sc.hadoopConfiguration.set("fs.s3a.stsAssumeRole.arn", "arn:aws:iam::522910024833:role/databricks-dev")

// COMMAND ----------

package com.cisco.ciscoImpact.datalake.streaming.deleteStream1

import com.cisco.utils.mongodb.JobContextMongo
import com.cisco.impact.analytics.spark.dbi.mongodb.MongoDbSession
import com.cisco.impact.analytics.spark.dbi.mongodb.implicits._
import com.cisco.datalake.model.{MongoAuth, MongoConfig}


@SerialVersionUID(800L)
class Params (@transient private val config: MongoConfig) extends Serializable {
    val notebookName = "START_CI_DELETE_STREAM_PROCESSING"
   
@transient val mongoDbSession = new MongoDbSession()
                                           .host(config.auth.host, config.auth.port.intValue)
                                           .database(config.datalakeMaster)
                                           .credentials(config.auth.username, config.auth.password)

    @transient private val context = mongoDbSession.jobContext
    @transient private val contextParameters = context.getparameters(notebookName)
    val fileLocation = contextParameters.getParameterString("incrementalFileLoc", "")
    // === Configurations for Kinesis streams ===
    val awsAccessKeyId = contextParameters.getParameterString("awsAccessKeyId", "")
    val awsSecretKey = contextParameters.getParameterString("awsSecretKey", "")
    val kinesisStreamName = contextParameters.getParameterString("kinesisStreamName", "")
    val kinesisEndpointUrl = contextParameters.getParameterString("kinesisEndpointUrl", "")
    val kinesisAppName = contextParameters.getParameterString("kinesisAppName", "")
    val initialPosition = "TRIM_HORIZON"
   
    //Configuration for Spark Streaming
    val batchIntervalSeconds = contextParameters.getParameterString("batchIntervalSeconds", "")
    val checkpointDir = contextParameters.getParameterString("checkpointDir", "")

  @transient val logTable = mongoDbSession.tableLazy("log.spark_application_logs") 
override def toString = f"$notebookName%s;$fileLocation%s;$awsAccessKeyId%s;$awsSecretKey%s;$kinesisStreamName%s;$kinesisEndpointUrl%s;$kinesisAppName%s;$initialPosition%s;$batchIntervalSeconds%s;$checkpointDir%s"
}



// COMMAND ----------

import com.cisco.ciscoImpact.datalake.streaming.deleteStream1._
import com.cisco.impact.analytics.spark.logger.WorkflowLogger
import com.cisco.datalake.model.{MongoConfig, MongoAuth}

val mongoKey = "dev"

val mongoConfig = spark.read.json("s3araw://s3-datalake-dev/system/mongo_config.json").as[MongoConfig]
@transient private val config = mongoConfig.filter(_.key == mongoKey).head


val params = new Params(config)

@transient val logger = new WorkflowLogger()
  .stage(params.notebookName)
  .mongodb(params.logTable)

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import com.cisco.utils.FileDirectory
import org.apache.spark.sql.functions._ 
import net.liftweb.json._

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import java.nio.ByteBuffer
import scala.util.Random
import com.amazonaws.regions.RegionUtils
import org.apache.spark.sql.Dataset
import spark.implicits._
import org.apache.spark.sql.streaming.Trigger


val messageSchema = new StructType().add("storeId", StringType).add("app", StringType).add("tenant", StringType)
  
// Function to create a new StreamingContext and set it up
val streamingInputDF = spark.readStream
  .format("kinesis")
  .option("streamName", params.kinesisStreamName)
  .option("endpointUrl",  params.kinesisEndpointUrl)
  .option("region", RegionUtils.getRegionByEndpoint(params.kinesisEndpointUrl).getName())
  .option("initialPosition", params.initialPosition)
  .option("awsAccessKey", params.awsAccessKeyId)
  .option("awsSecretKey", params.awsSecretKey)
  .option("maxFetchDuration", params.batchIntervalSeconds)
  .load()
  .select(from_json(col("data").cast("string"), messageSchema).alias("parsed_value"))


val payload = streamingInputDF
  .select($"parsed_value.storeId", $"parsed_value.app".as("appName"), $"parsed_value.tenant")
 
logger.start()

// # keep the size of shuffles small
spark.conf.set("spark.sql.shuffle.partitions", "2") 

val query = payload
  .writeStream
  //.trigger(Trigger.Once)
  .format("parquet")
  .option("checkpointLocation", params.checkpointDir)
  .option("path", params.fileLocation  + FileDirectory.getSubfolderNameFromDate() + ".parquet/")
  .outputMode("append") 
  .start()


// COMMAND ----------

//StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

// COMMAND ----------

/*val stopActiveContext = true
if (stopActiveContext) {    
  StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
} */