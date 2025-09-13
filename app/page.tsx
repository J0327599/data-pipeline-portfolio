"use client"

import { useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Mail,
  Phone,
  MapPin,
  Github,
  Linkedin,
  ExternalLink,
  Database,
  Cloud,
  BarChart3,
  Cpu,
  Code,
  Server,
  Menu,
  X,
  Home,
  User,
  Briefcase,
  FolderOpen,
  MessageSquare,
} from "lucide-react"

const projectDetails = {
  "fraud-detection": {
    title: "Real-time Fraud Detection Pipeline",
    problemStatement:
      "Financial institutions face the challenge of detecting fraudulent transactions in real-time while minimizing false positives that disrupt legitimate customer transactions. Traditional batch processing systems couldn't meet the sub-second response time requirements needed for real-time fraud prevention.",
    architecture: "/real-time-fraud-detection-architecture-diagram-sho.jpg",
    solution: {
      python: `# Real-time fraud detection using Spark Streaming
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
import json

class FraudDetectionPipeline:
    def __init__(self):
        self.spark = SparkSession.builder.appName("FraudDetection").getOrCreate()
        self.ssc = StreamingContext(self.spark.sparkContext, 1)
        self.model = self.load_ml_model()
    
    def process_transaction_stream(self):
        # Connect to Kafka stream
        kafka_stream = self.ssc.socketTextStream("kafka-broker", 9092)
        
        # Process each transaction
        transactions = kafka_stream.map(self.parse_transaction)
        fraud_scores = transactions.map(self.predict_fraud)
        
        # Filter high-risk transactions
        high_risk = fraud_scores.filter(lambda x: x['fraud_score'] > 0.8)
        high_risk.foreachRDD(self.alert_fraud_team)
        
        return fraud_scores
    
    def predict_fraud(self, transaction):
        features = self.extract_features(transaction)
        fraud_score = self.model.predict(features)
        
        return {
            'transaction_id': transaction['id'],
            'fraud_score': fraud_score,
            'timestamp': transaction['timestamp']
        }`,
      sql: `-- Fraud detection feature engineering queries
WITH transaction_features AS (
  SELECT 
    transaction_id,
    user_id,
    amount,
    merchant_category,
    -- Time-based features
    EXTRACT(HOUR FROM transaction_time) as hour_of_day,
    EXTRACT(DOW FROM transaction_time) as day_of_week,
    
    -- User behavior features
    COUNT(*) OVER (
      PARTITION BY user_id 
      ORDER BY transaction_time 
      RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
    ) as transactions_last_hour,
    
    AVG(amount) OVER (
      PARTITION BY user_id 
      ORDER BY transaction_time 
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) as avg_amount_last_10,
    
    -- Location features
    ST_Distance(
      transaction_location, 
      LAG(transaction_location) OVER (PARTITION BY user_id ORDER BY transaction_time)
    ) as distance_from_last_transaction
    
  FROM transactions 
  WHERE transaction_time >= NOW() - INTERVAL '24 hours'
),

fraud_scores AS (
  SELECT *,
    CASE 
      WHEN amount > avg_amount_last_10 * 5 THEN 0.3
      WHEN transactions_last_hour > 10 THEN 0.4
      WHEN distance_from_last_transaction > 1000 THEN 0.2
      ELSE 0.0
    END as risk_score
  FROM transaction_features
)

SELECT * FROM fraud_scores WHERE risk_score > 0.5;`,
      scala: `// Kafka consumer for real-time transaction processing
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer

object FraudDetectionStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FraudDetectionStream")
      .getOrCreate()
    
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka-broker:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "fraud-detection-group"
    )
    
    val topics = Array("transactions")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    
    // Process transactions and detect fraud
    val fraudDetections = stream.map(record => {
      val transaction = parseTransaction(record.value())
      val fraudScore = predictFraud(transaction)
      (transaction.id, fraudScore)
    })
    
    fraudDetections.foreachRDD { rdd =>
      rdd.filter(_._2 > 0.8).foreach { case (id, score) =>
        alertFraudTeam(id, score)
      }
    }
    
    ssc.start()
    ssc.awaitTermination()
  }
}`,
    },
  },
  "data-warehouse": {
    title: "Customer Analytics Data Warehouse",
    problemStatement:
      "The marketing team needed a centralized data warehouse to analyze customer behavior across multiple touchpoints (web, mobile, email, social media). Existing data was siloed across different systems, making it impossible to get a unified view of customer journeys and measure marketing campaign effectiveness.",
    architecture: "/data-warehouse-architecture-diagram-showing-etl-pi.jpg",
    solution: {
      sql: `-- Customer 360 view with behavioral analytics
CREATE TABLE customer_360 AS
WITH customer_base AS (
  SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    registration_date,
    customer_segment
  FROM customers
),

transaction_summary AS (
  SELECT 
    customer_id,
    COUNT(*) as total_transactions,
    SUM(amount) as total_spent,
    AVG(amount) as avg_transaction_value,
    MAX(transaction_date) as last_transaction_date,
    MIN(transaction_date) as first_transaction_date
  FROM transactions 
  GROUP BY customer_id
),

engagement_metrics AS (
  SELECT 
    customer_id,
    COUNT(CASE WHEN event_type = 'email_open' THEN 1 END) as email_opens,
    COUNT(CASE WHEN event_type = 'email_click' THEN 1 END) as email_clicks,
    COUNT(CASE WHEN event_type = 'website_visit' THEN 1 END) as website_visits,
    COUNT(CASE WHEN event_type = 'app_session' THEN 1 END) as app_sessions
  FROM customer_events 
  WHERE event_date >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY customer_id
)

SELECT 
  cb.*,
  ts.total_transactions,
  ts.total_spent,
  ts.avg_transaction_value,
  ts.last_transaction_date,
  DATEDIFF('day', ts.last_transaction_date, CURRENT_DATE) as days_since_last_purchase,
  em.email_opens,
  em.email_clicks,
  em.website_visits,
  em.app_sessions,
  
  -- Customer lifetime value calculation
  ts.total_spent / NULLIF(DATEDIFF('day', ts.first_transaction_date, CURRENT_DATE), 0) * 365 as estimated_clv,
  
  -- Engagement score
  (em.email_opens * 0.1 + em.email_clicks * 0.3 + em.website_visits * 0.2 + em.app_sessions * 0.4) as engagement_score

FROM customer_base cb
LEFT JOIN transaction_summary ts ON cb.customer_id = ts.customer_id
LEFT JOIN engagement_metrics em ON cb.customer_id = em.customer_id;`,
      python: `# Airflow DAG for customer data warehouse ETL
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'customer_analytics_etl',
    default_args=default_args,
    description='Customer analytics data warehouse ETL',
    schedule_interval='@daily',
    catchup=False
)

def extract_customer_data(**context):
    """Extract customer data from multiple sources"""
    # Extract from CRM system
    crm_data = extract_from_crm()
    
    # Extract from e-commerce platform
    ecommerce_data = extract_from_ecommerce()
    
    # Extract from marketing automation
    marketing_data = extract_from_marketing_platform()
    
    # Store raw data in S3
    s3_client = boto3.client('s3')
    
    # Upload to S3 staging area
    s3_client.put_object(
        Bucket='customer-data-lake',
        Key=f'raw/crm/{context["ds"]}/customers.parquet',
        Body=crm_data.to_parquet()
    )
    
    return f"Extracted {len(crm_data)} customer records"

def transform_customer_data(**context):
    """Transform and clean customer data"""
    s3_client = boto3.client('s3')
    
    # Read raw data from S3
    crm_data = pd.read_parquet(f's3://customer-data-lake/raw/crm/{context["ds"]}/customers.parquet')
    
    # Data cleaning and transformation
    transformed_data = (crm_data
        .drop_duplicates(subset=['customer_id'])
        .fillna({'customer_segment': 'Unknown'})
        .assign(
            full_name=lambda x: x['first_name'] + ' ' + x['last_name'],
            registration_month=lambda x: pd.to_datetime(x['registration_date']).dt.to_period('M')
        )
    )
    
    # Data quality checks
    assert transformed_data['customer_id'].nunique() == len(transformed_data), "Duplicate customer IDs found"
    assert transformed_data['email'].str.contains('@').all(), "Invalid email addresses found"
    
    # Save transformed data
    transformed_data.to_parquet(f's3://customer-data-lake/transformed/customers/{context["ds"]}/customers.parquet')
    
    return f"Transformed {len(transformed_data)} customer records"

# Define tasks
extract_task = PythonOperator(
    task_id='extract_customer_data',
    python_callable=extract_customer_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_customer_data',
    python_callable=transform_customer_data,
    dag=dag
)

load_task = PostgresOperator(
    task_id='load_to_redshift',
    postgres_conn_id='redshift_default',
    sql='''
        COPY customer_360 
        FROM 's3://customer-data-lake/transformed/customers/{{ ds }}/customers.parquet'
        IAM_ROLE 'arn:aws:iam::account:role/RedshiftRole'
        FORMAT AS PARQUET;
    ''',
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task`,
      yaml: `# dbt model for customer segmentation
# models/marts/customer_segmentation.sql
version: 2

models:
  - name: customer_segmentation
    description: "Customer segmentation based on RFM analysis"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - unique
          - not_null
      - name: rfm_segment
        description: "RFM-based customer segment"
        tests:
          - accepted_values:
              values: ['Champions', 'Loyal Customers', 'Potential Loyalists', 'At Risk', 'Cannot Lose Them']

# dbt_project.yml configuration
name: 'customer_analytics'
version: '1.0.0'
config-version: 2

model-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  customer_analytics:
    staging:
      +materialized: view
    marts:
      +materialized: table
      +post-hook: "GRANT SELECT ON {{ this }} TO ROLE analyst_role"`,
    },
  },
  "iot-platform": {
    title: "IoT Sensor Data Processing Platform",
    problemStatement:
      "Manufacturing facilities needed to process millions of IoT sensor readings per hour from industrial equipment to enable predictive maintenance and prevent costly downtime. The existing system couldn't handle the volume and velocity of sensor data, leading to delayed insights and missed maintenance opportunities.",
    architecture: "/iot-data-processing-architecture-with-kafka--spark.jpg",
    solution: {
      python: `# IoT sensor data processing with Spark Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class IoTDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("IoTSensorProcessing") \
            .config("spark.cassandra.connection.host", "cassandra-cluster") \
            .getOrCreate()
    
    def process_sensor_stream(self):
        # Define sensor data schema
        sensor_schema = StructType([
            StructField("sensor_id", StringType(), True),
            StructField("equipment_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("vibration", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("rotation_speed", DoubleType(), True)
        ])
        
        # Read from Kafka stream
        sensor_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka-cluster:9092") \
            .option("subscribe", "sensor-data") \
            .load()
        
        # Parse JSON data
        parsed_data = sensor_stream.select(
            from_json(col("value").cast("string"), sensor_schema).alias("data")
        ).select("data.*")
        
        # Add derived features for anomaly detection
        enriched_data = parsed_data.withColumn(
            "temp_anomaly", 
            when(col("temperature") > 85.0, 1).otherwise(0)
        ).withColumn(
            "vibration_anomaly",
            when(col("vibration") > 2.5, 1).otherwise(0)
        ).withColumn(
            "processing_time",
            current_timestamp()
        )
        
        # Windowed aggregations for trend analysis
        windowed_metrics = enriched_data \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("equipment_id")
            ).agg(
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                avg("vibration").alias("avg_vibration"),
                max("vibration").alias("max_vibration"),
                sum("temp_anomaly").alias("temp_anomaly_count"),
                sum("vibration_anomaly").alias("vibration_anomaly_count")
            )
        
        # Write to Cassandra for real-time queries
        query = enriched_data.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "iot_data") \
            .option("table", "sensor_readings") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()
        
        return query
    
    def detect_equipment_anomalies(self, df):
        """Detect equipment anomalies using statistical methods"""
        # Calculate z-scores for anomaly detection
        stats = df.select(
            mean("temperature").alias("temp_mean"),
            stddev("temperature").alias("temp_std"),
            mean("vibration").alias("vib_mean"),
            stddev("vibration").alias("vib_std")
        ).collect()[0]
        
        anomalies = df.withColumn(
            "temp_zscore",
            abs(col("temperature") - stats["temp_mean"]) / stats["temp_std"]
        ).withColumn(
            "vib_zscore", 
            abs(col("vibration") - stats["vib_mean"]) / stats["vib_std"]
        ).filter(
            (col("temp_zscore") > 3) | (col("vib_zscore") > 3)
        )
        
        return anomalies`,
      scala: `// Fixed Scala code syntax and imports
// Kafka Streams application for IoT data processing
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import java.util.Properties
import io.circe.parser._
import io.circe.generic.auto._
import java.time.Duration

case class SensorReading(
  sensorId: String,
  equipmentId: String,
  timestamp: Long,
  temperature: Double,
  vibration: Double,
  pressure: Double,
  rotationSpeed: Double
)

case class EquipmentAlert(
  equipmentId: String,
  alertType: String,
  severity: String,
  timestamp: Long,
  message: String
)

object IoTStreamProcessor extends App {
  import Serdes._
  
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "iot-processor")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-cluster:9092")
  
  val builder = new StreamsBuilder()
  
  // Source stream from sensor data topic
  val sensorStream: KStream[String, String] = builder.stream[String, String]("sensor-data")
  
  // Parse JSON and create sensor reading objects
  val parsedStream: KStream[String, SensorReading] = sensorStream
    .mapValues(value => decode[SensorReading](value))
    .filter((_, either) => either.isRight)
    .mapValues(either => either.right.get)
  
  // Detect temperature anomalies
  val temperatureAlerts: KStream[String, EquipmentAlert] = parsedStream
    .filter((_, sensorReading) => sensorReading.temperature > 85.0)
    .mapValues(sensorReading => EquipmentAlert(
      equipmentId = sensorReading.equipmentId,
      alertType = "TEMPERATURE_HIGH",
      severity = if (sensorReading.temperature > 95.0) "CRITICAL" else "WARNING",
      timestamp = sensorReading.timestamp,
      message = s"High temperature detected: \${sensorReading.temperature}°C"
    ))
  
  // Detect vibration anomalies
  val vibrationAlerts: KStream[String, EquipmentAlert] = parsedStream
    .filter((_, sensorReading) => sensorReading.vibration > 2.5)
    .mapValues(sensorReading => EquipmentAlert(
      equipmentId = sensorReading.equipmentId,
      alertType = "VIBRATION_HIGH", 
      severity = if (sensorReading.vibration > 4.0) "CRITICAL" else "WARNING",
      timestamp = sensorReading.timestamp,
      message = s"High vibration detected: \${sensorReading.vibration} Hz"
    ))
  
  // Combine all alerts
  val allAlerts = temperatureAlerts.merge(vibrationAlerts)
  
  // Send alerts to notification topic
  allAlerts.to("equipment-alerts")
  
  // Aggregate metrics by equipment over 5-minute windows
  val equipmentMetrics = parsedStream
    .groupByKey
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
    .aggregate(
      initializer = () => (0, 0.0, 0.0, 0.0, 0.0),
      aggregator = (key, sensorReading, agg) => (
        agg._1 + 1, // count
        agg._2 + sensorReading.temperature, // temp sum
        agg._3 + sensorReading.vibration, // vibration sum  
        math.max(agg._4, sensorReading.temperature), // max temp
        math.max(agg._5, sensorReading.vibration) // max vibration
      )
    )
    .mapValues { case (count, tempSum, vibSum, maxTemp, maxVib) =>
      s"count=$count,avgTemp=\${tempSum / count},avgVib=\${vibSum / count},maxTemp=$maxTemp,maxVib=$maxVib"
    }
    .toStream
    .to("equipment-metrics")
  
  val streams = new KafkaStreams(builder.build(), props)
  streams.start()
  
  sys.addShutdownHook {
    streams.close()
  }
}`,
      cql: `-- Cassandra schema for IoT sensor data
CREATE KEYSPACE IF NOT EXISTS iot_data 
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

USE iot_data;

-- Table for raw sensor readings
CREATE TABLE sensor_readings (
  equipment_id text,
  sensor_id text,
  timestamp timestamp,
  temperature double,
  vibration double,
  pressure double,
  rotation_speed double,
  temp_anomaly int,
  vibration_anomaly int,
  processing_time timestamp,
  PRIMARY KEY ((equipment_id), timestamp, sensor_id)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy'}
  AND default_time_to_live = 2592000; -- 30 days TTL

-- Table for equipment alerts
CREATE TABLE equipment_alerts (
  equipment_id text,
  alert_id uuid,
  timestamp timestamp,
  alert_type text,
  severity text,
  message text,
  acknowledged boolean,
  PRIMARY KEY ((equipment_id), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Table for aggregated metrics (5-minute windows)
CREATE TABLE equipment_metrics_5min (
  equipment_id text,
  window_start timestamp,
  window_end timestamp,
  avg_temperature double,
  max_temperature double,
  avg_vibration double,
  max_vibration double,
  temp_anomaly_count int,
  vibration_anomaly_count int,
  reading_count bigint,
  PRIMARY KEY ((equipment_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 7776000; -- 90 days TTL

-- Materialized view for recent alerts
CREATE MATERIALIZED VIEW recent_critical_alerts AS
  SELECT equipment_id, alert_id, timestamp, alert_type, message
  FROM equipment_alerts
  WHERE equipment_id IS NOT NULL 
    AND timestamp IS NOT NULL 
    AND alert_id IS NOT NULL
    AND severity = 'CRITICAL'
  PRIMARY KEY ((severity), timestamp, equipment_id, alert_id)
  WITH CLUSTERING ORDER BY (timestamp DESC);

-- Indexes for common query patterns
CREATE INDEX ON sensor_readings (sensor_id);
CREATE INDEX ON equipment_alerts (alert_type);
CREATE INDEX ON equipment_alerts (severity);`,
    },
  },
  "risk-analytics": {
    title: "Financial Risk Analytics Engine",
    problemStatement:
      "Financial institutions needed a comprehensive risk analytics platform to assess credit risk, market risk, and operational risk in real-time. Legacy systems took hours to process risk calculations, preventing timely decision-making and regulatory compliance. The solution needed to handle complex financial models and large datasets while providing sub-minute risk assessments.",
    architecture: "/financial-risk-analytics-architecture-showing-data.jpg",
    solution: {
      python: `# Financial risk analytics engine with PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import numpy as np

class RiskAnalyticsEngine:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RiskAnalyticsEngine") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def calculate_credit_risk(self, loan_data):
        """Calculate credit risk scores using logistic regression"""
        
        # Feature engineering for credit risk
        credit_features = loan_data.select(
            col("loan_id"),
            col("borrower_id"),
            col("loan_amount"),
            col("annual_income"),
            col("debt_to_income_ratio"),
            col("credit_score"),
            col("employment_length"),
            col("loan_purpose"),
            
            # Derived features
            (col("loan_amount") / col("annual_income")).alias("loan_to_income_ratio"),
            when(col("credit_score") >= 750, "excellent")
            .when(col("credit_score") >= 700, "good")
            .when(col("credit_score") >= 650, "fair")
            .otherwise("poor").alias("credit_grade"),
            
            # Risk indicators
            when(col("debt_to_income_ratio") > 0.4, 1).otherwise(0).alias("high_dti_flag"),
            when(col("employment_length") < 2, 1).otherwise(0).alias("short_employment_flag")
        )
        
        # Calculate probability of default using statistical model
        risk_scores = credit_features.withColumn(
            "base_risk_score",
            when(col("credit_score") < 600, 0.15)
            .when(col("credit_score") < 650, 0.08)
            .when(col("credit_score") < 700, 0.04)
            .when(col("credit_score") < 750, 0.02)
            .otherwise(0.01)
        ).withColumn(
            "adjusted_risk_score",
            col("base_risk_score") * 
            (1 + col("high_dti_flag") * 0.5) *
            (1 + col("short_employment_flag") * 0.3) *
            (1 + when(col("loan_to_income_ratio") > 5, 0.4).otherwise(0))
        ).withColumn(
            "risk_category",
            when(col("adjusted_risk_score") < 0.02, "Low")
            .when(col("adjusted_risk_score") < 0.05, "Medium")
            .when(col("adjusted_risk_score") < 0.10, "High")
            .otherwise("Very High")
        )
        
        return risk_scores
    
    def calculate_market_risk_var(self, portfolio_data, confidence_level=0.95):
        """Calculate Value at Risk (VaR) for market risk assessment"""
        
        # Calculate daily returns for each asset
        returns_data = portfolio_data.withColumn(
            "daily_return",
            (col("close_price") - col("prev_close_price")) / col("prev_close_price")
        )
        
        # Portfolio-level calculations
        portfolio_returns = returns_data.groupBy("date").agg(
            sum(col("daily_return") * col("position_value") / col("total_portfolio_value")).alias("portfolio_return")
        )
        
        # Calculate VaR using historical simulation
        var_calculation = portfolio_returns.select(
            col("date"),
            col("portfolio_return"),
            percent_rank().over(Window.orderBy("portfolio_return")).alias("percentile")
        ).filter(
            col("percentile") <= (1 - confidence_level)
        ).agg(
            max("portfolio_return").alias("var_95")
        )
        
        return var_calculation
    
    def operational_risk_assessment(self, self, operational_data):
        """Assess operational risk using key risk indicators"""
        
        operational_metrics = operational_data.select(
            col("business_unit"),
            col("date"),
            col("transaction_volume"),
            col("error_count"),
            col("system_downtime_minutes"),
            col("staff_count"),
            col("training_hours"),
            
            # Calculate risk indicators
            (col("error_count") / col("transaction_volume") * 100).alias("error_rate_pct"),
            (col("system_downtime_minutes") / 1440 * 100).alias("downtime_pct"),
            (col("training_hours") / col("staff_count")).alias("training_per_staff")
        ).withColumn(
            "operational_risk_score",
            col("error_rate_pct") * 0.4 +
            col("downtime_pct") * 0.3 +
            when(col("training_per_staff") < 40, 20).otherwise(0) * 0.3
        ).withColumn(
            "risk_level",
            when(col("operational_risk_score") < 5, "Low")
            .when(col("operational_risk_score") < 15, "Medium")
            .when(col("operational_risk_score") < 25, "High")
            .otherwise("Critical")
        )
        
        return operational_metrics
    
    def generate_risk_report(self, credit_risk, market_risk, operational_risk):
        """Generate comprehensive risk report"""
        
        # Aggregate risk metrics
        risk_summary = self.spark.sql("""
            SELECT 
                'Credit Risk' as risk_type,
                COUNT(*) as total_exposures,
                SUM(CASE WHEN risk_category = 'High' OR risk_category = 'Very High' THEN 1 ELSE 0 END) as high_risk_count,
                AVG(adjusted_risk_score) as avg_risk_score
            FROM credit_risk_temp
            
            UNION ALL
            
            SELECT 
                'Operational Risk' as risk_type,
                COUNT(*) as total_exposures,
                SUM(CASE WHEN risk_level = 'High' OR risk_level = 'Critical' THEN 1 ELSE 0 END) as high_risk_count,
                AVG(operational_risk_score) as avg_risk_score
            FROM operational_risk_temp
        """)
        
        return risk_summary`,
      sql: `-- Advanced risk analytics queries for financial institutions
-- Credit Risk Analysis with Cohort Analysis
WITH loan_cohorts AS (
  SELECT 
    DATE_TRUNC('month', origination_date) as cohort_month,
    loan_id,
    borrower_id,
    loan_amount,
    credit_score,
    debt_to_income_ratio,
    CASE 
      WHEN default_date IS NOT NULL THEN 1 
      ELSE 0 
    END as is_default,
    CASE 
      WHEN default_date IS NOT NULL 
      THEN DATE_PART('month', AGE(default_date, origination_date))
      ELSE DATE_PART('month', AGE(CURRENT_DATE, origination_date))
    END as months_since_origination
  FROM loans l
  WHERE origination_date >= '2020-01-01'
),

cohort_performance AS (
  SELECT 
    cohort_month,
    COUNT(*) as total_loans,
    SUM(loan_amount) as total_amount,
    SUM(is_default) as default_count,
    SUM(CASE WHEN is_default = 1 THEN loan_amount ELSE 0 END) as default_amount,
    
    -- Default rates by time periods
    SUM(CASE WHEN months_since_origination <= 6 AND is_default = 1 THEN 1 ELSE 0 END) as defaults_6m,
    SUM(CASE WHEN months_since_origination <= 12 AND is_default = 1 THEN 1 ELSE 0 END) as defaults_12m,
    SUM(CASE WHEN months_since_origination <= 24 AND is_default = 1 THEN 1 ELSE 0 END) as defaults_24m,
    
    -- Risk-adjusted metrics
    AVG(credit_score) as avg_credit_score,
    AVG(debt_to_income_ratio) as avg_dti,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY credit_score) as median_credit_score
    
  FROM loan_cohorts
  GROUP BY cohort_month
),

-- Market Risk - Portfolio VaR Calculation
portfolio_var AS (
  SELECT 
    portfolio_date,
    asset_class,
    SUM(market_value) as total_market_value,
    
    -- Calculate daily returns
    SUM(market_value * daily_return) / SUM(market_value) as weighted_return,
    
    -- Risk metrics
    STDDEV(daily_return) as volatility,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) as var_95,
    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY daily_return) as var_99,
    
    -- Expected Shortfall (Conditional VaR)
    AVG(CASE WHEN daily_return <= PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) 
             THEN daily_return ELSE NULL END) as expected_shortfall_95
             
  FROM portfolio_positions pp
  JOIN market_data md ON pp.asset_id = md.asset_id AND pp.portfolio_date = md.date
  WHERE portfolio_date >= CURRENT_DATE - INTERVAL '252 days' -- 1 year of trading days
  GROUP BY portfolio_date, asset_class
),

-- Operational Risk - Key Risk Indicators
operational_risk_kris AS (
  SELECT 
    business_unit,
    risk_date,
    
    -- Process Risk Indicators
    transaction_volume,
    error_count,
    CASE WHEN transaction_volume > 0 
         THEN (error_count::DECIMAL / transaction_volume) * 100 
         ELSE 0 END as error_rate,
    
    -- Technology Risk Indicators  
    system_downtime_minutes,
    (system_downtime_minutes::DECIMAL / 1440) * 100 as downtime_percentage,
    
    -- People Risk Indicators
    staff_turnover_count,
    total_staff_count,
    CASE WHEN total_staff_count > 0 
         THEN (staff_turnover_count::DECIMAL / total_staff_count) * 100 
         ELSE 0 END as turnover_rate,
    
    -- Compliance Risk Indicators
    regulatory_breaches,
    audit_findings,
    
    -- Calculate composite operational risk score
    (
      LEAST(error_rate * 2, 20) + -- Cap error rate impact at 20
      LEAST(downtime_percentage * 3, 30) + -- Cap downtime impact at 30  
      LEAST(turnover_rate * 1.5, 15) + -- Cap turnover impact at 15
      regulatory_breaches * 10 + -- Each breach adds 10 points
      audit_findings * 5 -- Each finding adds 5 points
    ) as operational_risk_score
    
  FROM operational_metrics om
  WHERE risk_date >= CURRENT_DATE - INTERVAL '90 days'
),

-- Risk Concentration Analysis
concentration_risk AS (
  SELECT 
    'Industry' as concentration_type,
    industry_sector as concentration_category,
    COUNT(*) as exposure_count,
    SUM(exposure_amount) as total_exposure,
    SUM(exposure_amount) / (SELECT SUM(exposure_amount) FROM credit_exposures) * 100 as concentration_pct,
    
    -- Herfindahl-Hirschman Index for concentration
    POWER(SUM(exposure_amount) / (SELECT SUM(exposure_amount) FROM credit_exposures) * 100, 2) as hhi_component
    
  FROM credit_exposures ce
  JOIN borrowers b ON ce.borrower_id = b.borrower_id
  GROUP BY industry_sector
  
  UNION ALL
  
  SELECT 
    'Geographic' as concentration_type,
    geographic_region as concentration_category,
    COUNT(*) as exposure_count,
    SUM(exposure_amount) as total_exposure,
    SUM(exposure_amount) / (SELECT SUM(exposure_amount) FROM credit_exposures) * 100 as concentration_pct,
    POWER(SUM(exposure_amount) / (SELECT SUM(exposure_amount) FROM credit_exposures) * 100, 2) as hhi_component
    
  FROM credit_exposures ce
  JOIN borrowers b ON ce.borrower_id = b.borrower_id
  GROUP BY geographic_region
)

-- Final Risk Dashboard Query
SELECT 
  'Credit Risk Summary' as metric_category,
  cohort_month::TEXT as metric_date,
  'Default Rate' as metric_name,
  (default_count::DECIMAL / total_loans * 100)::DECIMAL(5,2) as metric_value,
  '%' as metric_unit
FROM cohort_performance
WHERE cohort_month >= CURRENT_DATE - INTERVAL '12 months'

UNION ALL

SELECT 
  'Market Risk Summary' as metric_category,
  portfolio_date::TEXT as metric_date,
  'Portfolio VaR 95%' as metric_name,
  (var_95 * 100)::DECIMAL(5,2) as metric_value,
  '%' as metric_unit
FROM portfolio_var
WHERE portfolio_date = (SELECT MAX(portfolio_date) FROM portfolio_var)

UNION ALL

SELECT 
  'Operational Risk Summary' as metric_category,
  risk_date::TEXT as metric_date,
  'Operational Risk Score' as metric_name,
  operational_risk_score::DECIMAL(5,2) as metric_value,
  'points' as metric_unit
FROM operational_risk_kris
WHERE risk_date = (SELECT MAX(risk_date) FROM operational_risk_kris)

ORDER BY metric_category, metric_date DESC;`,
      r: `# Advanced Risk Analytics in R
library(dplyr)
library(ggplot2)
library(VaR)
library(RiskPortfolios)
library(quantmod)

# Credit Risk Modeling with Logistic Regression
credit_risk_model <- function(loan_data) {
  
  # Feature engineering
  loan_features <- loan_data %>%
    mutate(
      loan_to_income = loan_amount / annual_income,
      credit_grade = case_when(
        credit_score >= 750 ~ "Excellent",
        credit_score >= 700 ~ "Good", 
        credit_score >= 650 ~ "Fair",
        TRUE ~ "Poor"
      ),
      high_dti = ifelse(debt_to_income_ratio > 0.4, 1, 0),
      short_employment = ifelse(employment_length < 2, 1, 0)
    )
  
  # Logistic regression model for default prediction
  model <- glm(
    default_flag ~ credit_score + debt_to_income_ratio + loan_to_income + 
                  employment_length + high_dti + short_employment,
    data = loan_features,
    family = binomial(link = "logit")
  )
  
  # Generate predictions
  loan_features$default_probability <- predict(model, type = "response")
  loan_features$risk_category <- cut(
    loan_features$default_probability,
    breaks = c(0, 0.02, 0.05, 0.10, 1),
    labels = c("Low", "Medium", "High", "Very High"),
    include.lowest = TRUE
  )
  
  return(list(model = model, predictions = loan_features))
}

# Market Risk - Monte Carlo VaR Simulation
monte_carlo_var <- function(returns_data, portfolio_weights, confidence_level = 0.95, num_simulations = 10000) {
  
  # Calculate covariance matrix
  cov_matrix <- cov(returns_data)
  mean_returns <- colMeans(returns_data)
  
  # Monte Carlo simulation
  set.seed(123)
  simulated_returns <- matrix(0, nrow = num_simulations, ncol = length(mean_returns))
  
  for(i in 1:num_simulations) {
    simulated_returns[i, ] <- mvrnorm(1, mean_returns, cov_matrix)
  }
  
  # Calculate portfolio returns for each simulation
  portfolio_returns <- simulated_returns %*% portfolio_weights
  
  # Calculate VaR
  var_value <- quantile(portfolio_returns, 1 - confidence_level)
  expected_shortfall <- mean(portfolio_returns[portfolio_returns <= var_value])
  
  return(list(
    var = var_value,
    expected_shortfall = expected_shortfall,
    simulated_returns = portfolio_returns
  ))
}

# Operational Risk - Loss Distribution Approach
operational_risk_lda <- function(loss_data) {
  
  # Frequency modeling (Poisson distribution)
  annual_frequency <- loss_data %>%
    group_by(year) %>%
    summarise(loss_count = n()) %>%
    pull(loss_count)
  
  frequency_lambda <- mean(annual_frequency)
  
  # Severity modeling (Log-normal distribution)
  loss_amounts <- loss_data$loss_amount[loss_data$loss_amount > 0]
  log_losses <- log(loss_amounts)
  
  severity_params <- list(
    meanlog = mean(log_losses),
    sdlog = sd(log_losses)
  )
  
  # Monte Carlo simulation for annual operational loss
  set.seed(456)
  num_simulations <- 10000
  annual_losses <- numeric(num_simulations)
  
  for(i in 1:num_simulations) {
    # Simulate number of losses
    num_losses <- rpois(1, frequency_lambda)
    
    if(num_losses > 0) {
      # Simulate loss amounts
      loss_amounts <- rlnorm(num_losses, severity_params$meanlog, severity_params$sdlog)
      annual_losses[i] <- sum(loss_amounts)
    }
  }
  
  # Calculate operational risk capital
  op_var_99_9 <- quantile(annual_losses, 0.999)
  expected_loss <- mean(annual_losses)
  unexpected_loss <- op_var_99_9 - expected_loss
  
  return(list(
    expected_annual_loss = expected_loss,
    operational_var_99_9 = op_var_99_9,
    unexpected_loss = unexpected_loss,
    simulated_losses = annual_losses
  ))
}

# Risk Reporting Dashboard
generate_risk_dashboard <- function(credit_results, market_results, operational_results) {
  
  # Create summary metrics
  risk_summary <- data.frame(
    Risk_Type = c("Credit", "Market", "Operational"),
    Key_Metric = c(
      paste0("High Risk Loans: ", sum(credit_results$predictions$risk_category %in% c("High", "Very High"))),
      paste0("Portfolio VaR 95%: $", format(abs(market_results$var), big.mark = ",")),
      paste0("Op Risk Capital: $", format(operational_results$unexpected_loss, big.mark = ","))
    ),
    Status = c("Monitor", "Within Limits", "Review Required")
  )
  
  # Visualization
  p1 <- ggplot(credit_results$predictions, aes(x = risk_category, fill = risk_category)) +
    geom_bar() +
    labs(title = "Credit Risk Distribution", x = "Risk Category", y = "Number of Loans") +
    theme_minimal()
  
  p2 <- ggplot(data.frame(returns = market_results$simulated_returns), aes(x = returns)) +
    geom_histogram(bins = 50, alpha = 0.7) +
    geom_vline(xintercept = market_results$var, color = "red", linetype = "dashed") +
    labs(title = "Market Risk - Simulated Portfolio Returns", x = "Portfolio Return", y = "Frequency") +
    theme_minimal()
  
  p3 <- ggplot(data.frame(losses = operational_results$simulated_losses), aes(x = losses)) +
    geom_histogram(bins = 50, alpha = 0.7) +
    geom_vline(xintercept = operational_results$operational_var_99_9, color = "red", linetype = "dashed") +
    labs(title = "Operational Risk - Annual Loss Distribution", x = "Annual Loss", y = "Frequency") +
    theme_minimal()
  
  return(list(
    summary = risk_summary,
    plots = list(credit = p1, market = p2, operational = p3)
  ))
}
`,
    },
  },
}

export default function Portfolio() {
  const [selectedProject, setSelectedProject] = useState<string | null>(null)
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false)

  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId)
    if (element) {
      element.scrollIntoView({ behavior: "smooth" })
      setIsMobileMenuOpen(false) // Close mobile menu after navigation
    }
  }

  return (
    <div className="min-h-screen bg-background">
      <nav className="fixed top-0 left-0 right-0 z-50 bg-background/95 backdrop-blur-sm border-b">
        <div className="max-w-6xl mx-auto px-4 py-3">
          <div className="flex items-center justify-between">
            <div className="font-serif font-bold text-xl text-primary">SE</div>

            {/* Desktop Navigation */}
            <div className="hidden md:flex items-center space-x-8">
              <button
                onClick={() => scrollToSection("home")}
                className="text-sm font-medium hover:text-primary transition-colors"
              >
                Home
              </button>
              <button
                onClick={() => scrollToSection("skills")}
                className="text-sm font-medium hover:text-primary transition-colors"
              >
                Skills
              </button>
              <button
                onClick={() => scrollToSection("experience")}
                className="text-sm font-medium hover:text-primary transition-colors"
              >
                Experience
              </button>
              <button
                onClick={() => scrollToSection("projects")}
                className="text-sm font-medium hover:text-primary transition-colors"
              >
                Projects
              </button>
              <button
                onClick={() => scrollToSection("contact")}
                className="text-sm font-medium hover:text-primary transition-colors"
              >
                Contact
              </button>
            </div>

            {/* Mobile Menu Button */}
            <button
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
              className="md:hidden p-2 hover:bg-muted rounded-lg transition-colors"
              aria-label="Toggle menu"
            >
              {isMobileMenuOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
            </button>
          </div>

          {/* Mobile Navigation Menu */}
          {isMobileMenuOpen && (
            <div className="md:hidden mt-4 pb-4 border-t">
              <div className="flex flex-col space-y-3 pt-4">
                <button
                  onClick={() => scrollToSection("home")}
                  className="flex items-center space-x-3 text-sm font-medium hover:text-primary transition-colors py-2"
                >
                  <Home className="h-4 w-4" />
                  <span>Home</span>
                </button>
                <button
                  onClick={() => scrollToSection("skills")}
                  className="flex items-center space-x-3 text-sm font-medium hover:text-primary transition-colors py-2"
                >
                  <User className="h-4 w-4" />
                  <span>Skills</span>
                </button>
                <button
                  onClick={() => scrollToSection("experience")}
                  className="flex items-center space-x-3 text-sm font-medium hover:text-primary transition-colors py-2"
                >
                  <Briefcase className="h-4 w-4" />
                  <span>Experience</span>
                </button>
                <button
                  onClick={() => scrollToSection("projects")}
                  className="flex items-center space-x-3 text-sm font-medium hover:text-primary transition-colors py-2"
                >
                  <FolderOpen className="h-4 w-4" />
                  <span>Projects</span>
                </button>
                <button
                  onClick={() => scrollToSection("contact")}
                  className="flex items-center space-x-3 text-sm font-medium hover:text-primary transition-colors py-2"
                >
                  <MessageSquare className="h-4 w-4" />
                  <span>Contact</span>
                </button>
              </div>
            </div>
          )}
        </div>
      </nav>

      {/* Hero Section */}
      <section id="home" className="relative py-20 px-4 text-center bg-gradient-to-br from-muted to-card pt-32">
        <div className="max-w-4xl mx-auto">
          <div className="mb-8">
            <img
              src="/images/stanton-edwards-professional.jpg"
              alt="Stanton Edwards - Professional headshot"
              className="w-32 h-32 md:w-40 md:h-40 rounded-full mx-auto object-cover object-[center_10%] border-4 border-primary/20 shadow-lg"
            />
          </div>
          <h1 className="text-5xl md:text-7xl font-serif font-bold text-foreground mb-6 text-balance">
            Stanton Edwards
          </h1>
          <p className="text-xl md:text-2xl text-muted-foreground mb-8 text-pretty">
            Senior Data Engineer | Big Data Enthusiast | Analytics Expert
          </p>
          <p className="text-lg text-card-foreground max-w-2xl mx-auto mb-8 leading-relaxed text-pretty">
            Transforming raw data into actionable business insights through scalable infrastructure, advanced analytics,
            and cutting-edge big data technologies.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button size="lg" className="bg-primary hover:bg-primary/90">
              <Mail className="mr-2 h-4 w-4" />
              Get In Touch
            </Button>
            <Button variant="outline" size="lg">
              <ExternalLink className="mr-2 h-4 w-4" />
              View Resume
            </Button>
          </div>
        </div>
      </section>

      {/* Skills Section */}
      <section id="skills" className="py-16 px-4">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-4xl font-serif font-bold text-center mb-12 text-balance">Technical Expertise</h2>
          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-8">
            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <Database className="h-8 w-8 text-primary" />
                  <CardTitle>Big Data Technologies</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Apache Spark</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Hadoop Ecosystem</span>
                      <span className="text-sm text-muted-foreground">90%</span>
                    </div>
                    <Progress value={90} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Apache Kafka</span>
                      <span className="text-sm text-muted-foreground">88%</span>
                    </div>
                    <Progress value={88} className="h-2" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <Cloud className="h-8 w-8 text-primary" />
                  <CardTitle>Cloud Platforms</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>AWS (EC2, EMR, Redshift)</span>
                      <span className="text-sm text-muted-foreground">92%</span>
                    </div>
                    <Progress value={92} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Data Pipeline Tools</span>
                      <span className="text-sm text-muted-foreground">85%</span>
                    </div>
                    <Progress value={85} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Stream Processing</span>
                      <span className="text-sm text-muted-foreground">87%</span>
                    </div>
                    <Progress value={87} className="h-2" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <Code className="h-8 w-8 text-primary" />
                  <CardTitle>Programming Languages</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Python</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>SQL</span>
                      <span className="text-sm text-muted-foreground">93%</span>
                    </div>
                    <Progress value={93} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Scala</span>
                      <span className="text-sm text-muted-foreground">80%</span>
                    </div>
                    <Progress value={80} className="h-2" />
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      {/* Experience Section */}
      <section id="experience" className="py-16 px-4">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-4xl font-serif font-bold text-center mb-12 text-balance">Professional Experience</h2>
          <div className="space-y-8">
            <Card>
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">Senior Data Engineer</CardTitle>
                    <CardDescription className="text-base">TechCorp Solutions • 2021 - Present</CardDescription>
                  </div>
                  <Badge variant="outline" className="w-fit">
                    Current Role
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>
                    • Led the design and implementation of a multi-petabyte data lake on AWS S3 with automated data
                    governance
                  </li>
                  <li>• Optimized Spark jobs reducing processing time by 70% and infrastructure costs by 45%</li>
                  <li>
                    • Mentored a team of 5 junior engineers and established best practices for data pipeline development
                  </li>
                  <li>
                    • Implemented real-time streaming analytics processing 50M+ events daily with sub-second latency
                  </li>
                </ul>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">Data Engineer</CardTitle>
                    <CardDescription className="text-base">DataFlow Analytics • 2019 - 2021</CardDescription>
                  </div>
                  <Badge variant="outline" className="w-fit">
                    2 Years
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>• Built and maintained ETL pipelines processing 100GB+ daily using Apache Airflow and Python</li>
                  <li>
                    • Migrated legacy data warehouse to cloud-native architecture improving query performance by 10x
                  </li>
                  <li>• Developed automated data quality monitoring reducing data incidents by 80%</li>
                  <li>• Collaborated with data scientists to productionize ML models serving 1M+ predictions daily</li>
                </ul>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">Junior Data Analyst</CardTitle>
                    <CardDescription className="text-base">StartupTech Inc • 2017 - 2019</CardDescription>
                  </div>
                  <Badge variant="outline" className="w-fit">
                    2 Years
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>• Developed automated reporting dashboards using Tableau and SQL reducing manual work by 90%</li>
                  <li>• Performed statistical analysis on customer data identifying key growth opportunities</li>
                  <li>• Built data validation scripts ensuring 99.9% data accuracy across all business metrics</li>
                  <li>• Created comprehensive documentation for data processes and analytical methodologies</li>
                </ul>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      {/* Projects Section */}
      <section id="projects" className="py-16 px-4 bg-muted/30">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-4xl font-serif font-bold text-center mb-12 text-balance">Featured Projects</h2>
          <div className="grid md:grid-cols-2 gap-8">
            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Real-time Fraud Detection Pipeline</CardTitle>
                    <CardDescription className="text-base">
                      Built a scalable real-time fraud detection system processing 1M+ transactions daily
                    </CardDescription>
                  </div>
                  <BarChart3 className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Designed and implemented a real-time fraud detection pipeline using Apache Kafka, Spark Streaming,
                    and machine learning models. Reduced false positives by 40% and improved detection accuracy to
                    98.5%.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">Apache Kafka</Badge>
                    <Badge variant="secondary">Spark Streaming</Badge>
                    <Badge variant="secondary">Python</Badge>
                    <Badge variant="secondary">AWS EMR</Badge>
                    <Badge variant="secondary">PostgreSQL</Badge>
                  </div>
                  <div className="pt-2">
                    <Dialog>
                      <DialogTrigger asChild>
                        <Button variant="outline" size="sm" onClick={() => setSelectedProject("fraud-detection")}>
                          <ExternalLink className="mr-2 h-3 w-3" />
                          View Details
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
                        <DialogHeader>
                          <DialogTitle className="text-2xl font-serif">
                            {projectDetails["fraud-detection"].title}
                          </DialogTitle>
                        </DialogHeader>
                        <Tabs defaultValue="problem" className="w-full">
                          <TabsList className="grid w-full grid-cols-4">
                            <TabsTrigger value="problem">Problem</TabsTrigger>
                            <TabsTrigger value="architecture">Architecture</TabsTrigger>
                            <TabsTrigger value="solution">Solution</TabsTrigger>
                            <TabsTrigger value="code">Code</TabsTrigger>
                          </TabsList>
                          <TabsContent value="problem" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Problem Statement</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <p className="text-muted-foreground leading-relaxed">
                                  {projectDetails["fraud-detection"].problemStatement}
                                </p>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="architecture" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>System Architecture</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <img
                                  src={projectDetails["fraud-detection"].architecture || "/placeholder.svg"}
                                  alt="Fraud Detection Architecture"
                                  className="w-full rounded-lg border"
                                />
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="solution" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Technical Solution</CardTitle>
                              </CardHeader>
                              <CardContent className="space-y-4">
                                <div className="grid md:grid-cols-2 gap-4">
                                  <div>
                                    <h4 className="font-semibold mb-2">Key Components:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Real-time data ingestion with Apache Kafka</li>
                                      <li>• Stream processing using Spark Streaming</li>
                                      <li>• Machine learning models for fraud detection</li>
                                      <li>• PostgreSQL for transaction storage</li>
                                      <li>• AWS EMR for scalable processing</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• 98.5% fraud detection accuracy</li>
                                      <li>• 40% reduction in false positives</li>
                                      <li>• Sub-second processing latency</li>
                                      <li>• 1M+ transactions processed daily</li>
                                      <li>• 99.9% system uptime</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Tabs defaultValue="python" className="w-full">
                              <TabsList>
                                <TabsTrigger value="python">Python</TabsTrigger>
                                <TabsTrigger value="sql">SQL</TabsTrigger>
                                <TabsTrigger value="scala">Scala</TabsTrigger>
                              </TabsList>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - Spark Streaming Implementation
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["fraud-detection"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="sql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      SQL - Feature Engineering
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["fraud-detection"].solution.sql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="scala">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Server className="h-5 w-5" />
                                      Scala - Kafka Streams
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["fraud-detection"].solution.scala}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                            </Tabs>
                          </TabsContent>
                        </Tabs>
                      </DialogContent>
                    </Dialog>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Customer Analytics Data Warehouse</CardTitle>
                    <CardDescription className="text-base">
                      Architected a comprehensive data warehouse for customer behavior analytics
                    </CardDescription>
                  </div>
                  <Server className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Built a multi-terabyte data warehouse on AWS Redshift with automated ETL pipelines using Airflow.
                    Enabled advanced customer segmentation and increased marketing ROI by 35%.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">AWS Redshift</Badge>
                    <Badge variant="secondary">Apache Airflow</Badge>
                    <Badge variant="secondary">dbt</Badge>
                    <Badge variant="secondary">Tableau</Badge>
                    <Badge variant="secondary">SQL</Badge>
                  </div>
                  <div className="pt-2">
                    <Dialog>
                      <DialogTrigger asChild>
                        <Button variant="outline" size="sm">
                          <ExternalLink className="mr-2 h-3 w-3" />
                          View Details
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
                        <DialogHeader>
                          <DialogTitle className="text-2xl font-serif">
                            {projectDetails["data-warehouse"].title}
                          </DialogTitle>
                        </DialogHeader>
                        <Tabs defaultValue="problem" className="w-full">
                          <TabsList className="grid w-full grid-cols-4">
                            <TabsTrigger value="problem">Problem</TabsTrigger>
                            <TabsTrigger value="architecture">Architecture</TabsTrigger>
                            <TabsTrigger value="solution">Solution</TabsTrigger>
                            <TabsTrigger value="code">Code</TabsTrigger>
                          </TabsList>
                          <TabsContent value="problem" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Problem Statement</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <p className="text-muted-foreground leading-relaxed">
                                  {projectDetails["data-warehouse"].problemStatement}
                                </p>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="architecture" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>System Architecture</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <img
                                  src={projectDetails["data-warehouse"].architecture || "/placeholder.svg"}
                                  alt="Data Warehouse Architecture"
                                  className="w-full rounded-lg border"
                                />
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="solution" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Technical Solution</CardTitle>
                              </CardHeader>
                              <CardContent className="space-y-4">
                                <div className="grid md:grid-cols-2 gap-4">
                                  <div>
                                    <h4 className="font-semibold mb-2">Key Components:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• AWS Redshift data warehouse</li>
                                      <li>• Apache Airflow for ETL orchestration</li>
                                      <li>• dbt for data transformation</li>
                                      <li>• Tableau for visualization</li>
                                      <li>• Multi-source data integration</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• 35% increase in marketing ROI</li>
                                      <li>• Multi-terabyte data processing</li>
                                      <li>• 360-degree customer view</li>
                                      <li>• Automated daily ETL processes</li>
                                      <li>• Real-time analytics dashboards</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Tabs defaultValue="sql" className="w-full">
                              <TabsList>
                                <TabsTrigger value="sql">SQL</TabsTrigger>
                                <TabsTrigger value="python">Python</TabsTrigger>
                                <TabsTrigger value="yaml">dbt</TabsTrigger>
                              </TabsList>
                              <TabsContent value="sql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      SQL - Customer 360 Analytics
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["data-warehouse"].solution.sql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - Airflow ETL Pipeline
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["data-warehouse"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="yaml">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Server className="h-5 w-5" />
                                      dbt - Data Transformation
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["data-warehouse"].solution.yaml}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                            </Tabs>
                          </TabsContent>
                        </Tabs>
                      </DialogContent>
                    </Dialog>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">IoT Sensor Data Processing Platform</CardTitle>
                    <CardDescription className="text-base">
                      Developed a scalable platform for processing millions of IoT sensor readings
                    </CardDescription>
                  </div>
                  <Cpu className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Created a distributed system handling 10M+ sensor readings per hour using Kafka, Spark, and
                    Cassandra. Implemented predictive maintenance algorithms reducing downtime by 25%.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">Apache Cassandra</Badge>
                    <Badge variant="secondary">Apache Spark</Badge>
                    <Badge variant="secondary">Kafka Connect</Badge>
                    <Badge variant="secondary">Docker</Badge>
                    <Badge variant="secondary">Kubernetes</Badge>
                  </div>
                  <div className="pt-2">
                    <Dialog>
                      <DialogTrigger asChild>
                        <Button variant="outline" size="sm">
                          <ExternalLink className="mr-2 h-3 w-3" />
                          View Details
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
                        <DialogHeader>
                          <DialogTitle className="text-2xl font-serif">
                            {projectDetails["iot-platform"].title}
                          </DialogTitle>
                        </DialogHeader>
                        <Tabs defaultValue="problem" className="w-full">
                          <TabsList className="grid w-full grid-cols-4">
                            <TabsTrigger value="problem">Problem</TabsTrigger>
                            <TabsTrigger value="architecture">Architecture</TabsTrigger>
                            <TabsTrigger value="solution">Solution</TabsTrigger>
                            <TabsTrigger value="code">Code</TabsTrigger>
                          </TabsList>
                          <TabsContent value="problem" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Problem Statement</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <p className="text-muted-foreground leading-relaxed">
                                  {projectDetails["iot-platform"].problemStatement}
                                </p>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="architecture" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>System Architecture</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <img
                                  src={projectDetails["iot-platform"].architecture || "/placeholder.svg"}
                                  alt="IoT Platform Architecture"
                                  className="w-full rounded-lg border"
                                />
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="solution" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Technical Solution</CardTitle>
                              </CardHeader>
                              <CardContent className="space-y-4">
                                <div className="grid md:grid-cols-2 gap-4">
                                  <div>
                                    <h4 className="font-semibold mb-2">Key Components:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Apache Cassandra for time-series data</li>
                                      <li>• Kafka for real-time data streaming</li>
                                      <li>• Spark for stream processing</li>
                                      <li>• Docker & Kubernetes deployment</li>
                                      <li>• Predictive maintenance algorithms</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• 10M+ sensor readings per hour</li>
                                      <li>• 25% reduction in equipment downtime</li>
                                      <li>• Real-time anomaly detection</li>
                                      <li>• Scalable microservices architecture</li>
                                      <li>• 99.95% system availability</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Tabs defaultValue="python" className="w-full">
                              <TabsList>
                                <TabsTrigger value="python">Python</TabsTrigger>
                                <TabsTrigger value="scala">Scala</TabsTrigger>
                                <TabsTrigger value="cql">CQL</TabsTrigger>
                              </TabsList>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - IoT Data Processing
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["iot-platform"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="scala">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Server className="h-5 w-5" />
                                      Scala - Kafka Streams Processing
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["iot-platform"].solution.scala}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="cql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      CQL - Cassandra Schema
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["iot-platform"].solution.cql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                            </Tabs>
                          </TabsContent>
                        </Tabs>
                      </DialogContent>
                    </Dialog>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Financial Risk Analytics Engine</CardTitle>
                    <CardDescription className="text-base">
                      Built a comprehensive risk analytics platform for financial institutions
                    </CardDescription>
                  </div>
                  <BarChart3 className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Developed a risk analytics engine processing complex financial data using advanced statistical
                    models. Improved risk assessment accuracy by 30% and reduced processing time by 60%.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">PySpark</Badge>
                    <Badge variant="secondary">Hadoop HDFS</Badge>
                    <Badge variant="secondary">Apache Hive</Badge>
                    <Badge variant="secondary">R</Badge>
                    <Badge variant="secondary">AWS S3</Badge>
                  </div>
                  <div className="pt-2">
                    <Dialog>
                      <DialogTrigger asChild>
                        <Button variant="outline" size="sm">
                          <ExternalLink className="mr-2 h-3 w-3" />
                          View Details
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
                        <DialogHeader>
                          <DialogTitle className="text-2xl font-serif">
                            {projectDetails["risk-analytics"].title}
                          </DialogTitle>
                        </DialogHeader>
                        <Tabs defaultValue="problem" className="w-full">
                          <TabsList className="grid w-full grid-cols-4">
                            <TabsTrigger value="problem">Problem</TabsTrigger>
                            <TabsTrigger value="architecture">Architecture</TabsTrigger>
                            <TabsTrigger value="solution">Solution</TabsTrigger>
                            <TabsTrigger value="code">Code</TabsTrigger>
                          </TabsList>
                          <TabsContent value="problem" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Problem Statement</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <p className="text-muted-foreground leading-relaxed">
                                  {projectDetails["risk-analytics"].problemStatement}
                                </p>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="architecture" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>System Architecture</CardTitle>
                              </CardHeader>
                              <CardContent>
                                <img
                                  src={projectDetails["risk-analytics"].architecture || "/placeholder.svg"}
                                  alt="Risk Analytics Architecture"
                                  className="w-full rounded-lg border"
                                />
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="solution" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle>Technical Solution</CardTitle>
                              </CardHeader>
                              <CardContent className="space-y-4">
                                <div className="grid md:grid-cols-2 gap-4">
                                  <div>
                                    <h4 className="font-semibold mb-2">Key Components:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• PySpark for distributed processing</li>
                                      <li>• Advanced statistical models in R</li>
                                      <li>• Hadoop HDFS for data storage</li>
                                      <li>• Apache Hive for data warehousing</li>
                                      <li>• AWS S3 for backup and archival</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• 30% improvement in risk assessment accuracy</li>
                                      <li>• 60% reduction in processing time</li>
                                      <li>• Real-time risk monitoring</li>
                                      <li>• Regulatory compliance automation</li>
                                      <li>• Multi-risk type analysis</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Tabs defaultValue="python" className="w-full">
                              <TabsList>
                                <TabsTrigger value="python">Python</TabsTrigger>
                                <TabsTrigger value="sql">SQL</TabsTrigger>
                                <TabsTrigger value="r">R</TabsTrigger>
                              </TabsList>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - Risk Analytics Engine
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["risk-analytics"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="sql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      SQL - Advanced Risk Analytics
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["risk-analytics"].solution.sql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="r">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <BarChart3 className="h-5 w-5" />R - Statistical Risk Modeling
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["risk-analytics"].solution.r}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                            </Tabs>
                          </TabsContent>
                        </Tabs>
                      </DialogContent>
                    </Dialog>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      {/* Contact Section */}
      <section id="contact" className="py-16 px-4 bg-muted/30">
        <div className="max-w-4xl mx-auto text-center">
          <h2 className="text-4xl font-serif font-bold mb-8 text-balance">Let's Build Something Amazing</h2>
          <p className="text-lg text-muted-foreground mb-8 max-w-2xl mx-auto leading-relaxed text-pretty">
            Ready to transform your data challenges into competitive advantages? Let's discuss how we can leverage big
            data technologies to drive your business forward.
          </p>

          <div className="grid md:grid-cols-3 gap-6 mb-8">
            <Card className="text-center">
              <CardContent className="pt-6">
                <Mail className="h-8 w-8 text-primary mx-auto mb-3" />
                <h3 className="font-semibold mb-2">Email</h3>
                <p className="text-sm text-muted-foreground">stanton.edwards@email.com</p>
              </CardContent>
            </Card>

            <Card className="text-center">
              <CardContent className="pt-6">
                <Phone className="h-8 w-8 text-primary mx-auto mb-3" />
                <h3 className="font-semibold mb-2">Phone</h3>
                <p className="text-sm text-muted-foreground">+1 (555) 123-4567</p>
              </CardContent>
            </Card>

            <Card className="text-center">
              <CardContent className="pt-6">
                <MapPin className="h-8 w-8 text-primary mx-auto mb-3" />
                <h3 className="font-semibold mb-2">Location</h3>
                <p className="text-sm text-muted-foreground">San Francisco, CA</p>
              </CardContent>
            </Card>
          </div>

          <div className="flex justify-center gap-4">
            <Button size="lg" className="bg-primary hover:bg-primary/90">
              <Mail className="mr-2 h-4 w-4" />
              Send Message
            </Button>
            <Button variant="outline" size="lg">
              <Github className="mr-2 h-4 w-4" />
              GitHub
            </Button>
            <Button variant="outline" size="lg">
              <Linkedin className="mr-2 h-4 w-4" />
              LinkedIn
            </Button>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="py-8 px-4 border-t">
        <div className="max-w-6xl mx-auto text-center">
          <p className="text-sm text-muted-foreground">
            © 2024 Stanton Edwards. Built with passion for data engineering and analytics.
          </p>
        </div>
      </footer>
    </div>
  )
}
