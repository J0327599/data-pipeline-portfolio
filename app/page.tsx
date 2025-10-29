"use client"

import { useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Award,
  Brain,
  Briefcase,
  Cloud,
  Cpu,
  Code,
  Database,
  ExternalLink,
  FolderOpen,
  Home,
  Mail,
  MapPin,
  Menu,
  MessageSquare,
  Phone,
  Server,
  TrendingUp,
  Users,
  X,
  BarChart3,
  FileText,
  FileSpreadsheet,
} from "lucide-react"

// Define ProjectDetail interface
interface ProjectDetail {
  title: string
  problemStatement: string
  architecture: string | undefined
  solution: {
    sql?: string
    python?: string
    scala?: string
    yaml?: string
    cql?: string
    r?: string
    msaccess?: string
    powerpoint?: string
  }
}

const projectDetails: Record<string, ProjectDetail> = {
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
       সুবিধlag(transaction_location) OVER (PARTITION BY user_id ORDER BY transaction_time)
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
  "hr-data-platform": {
    title: "Enterprise HR Data Platform",
    problemStatement:
      "The HR department managed employee data across 8 different HRIS and payroll systems (SAP SuccessFactors, Workday, ADP, local payroll systems), making it impossible to get a unified view of workforce analytics, ensure compliance with labor regulations, or generate accurate executive reports. Data inconsistencies, manual reconciliation processes, and lack of data governance led to reporting delays of up to 2 weeks and compliance risks.",
    architecture: "/hr-data-platform-architecture-medallion-bronze-silv.jpg",
    solution: {
      sql: `-- HR Data Platform - Relational Schema Design
-- Bronze Layer: Raw data ingestion from source systems

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- Bronze: Raw employee data from multiple HRIS systems
CREATE TABLE bronze.employee_raw (
    source_system VARCHAR(50),
    employee_id VARCHAR(100),
    load_timestamp DATETIME2,
    raw_data NVARCHAR(MAX), -- JSON payload
    batch_id VARCHAR(50),
    CONSTRAINT pk_employee_raw PRIMARY KEY (source_system, employee_id, load_timestamp)
);

-- Silver Layer: Cleaned and standardized data
CREATE TABLE silver.employee_master (
    employee_key INT IDENTITY(1,1) PRIMARY KEY,
    employee_id VARCHAR(50) UNIQUE NOT NULL,
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    email VARCHAR(255),
    hire_date DATE,
    termination_date DATE,
    employment_status VARCHAR(20),
    job_title NVARCHAR(200),
    department NVARCHAR(100),
    cost_center VARCHAR(50),
    manager_id VARCHAR(50),
    location_code VARCHAR(20),
    salary_grade VARCHAR(10),
    -- Audit columns
    source_system VARCHAR(50),
    created_date DATETIME2 DEFAULT GETDATE(),
    modified_date DATETIME2 DEFAULT GETDATE(),
    data_quality_score DECIMAL(3,2),
    is_active BIT DEFAULT 1,
    CONSTRAINT fk_manager FOREIGN KEY (manager_id) REFERENCES silver.employee_master(employee_id)
);

CREATE TABLE silver.payroll_transactions (
    payroll_key INT IDENTITY(1,1) PRIMARY KEY,
    employee_id VARCHAR(50) NOT NULL,
    pay_period_start DATE,
    pay_period_end DATE,
    pay_date DATE,
    gross_pay DECIMAL(18,2),
    net_pay DECIMAL(18,2),
    tax_withheld DECIMAL(18,2),
    benefits_deduction DECIMAL(18,2),
    currency_code VARCHAR(3),
    source_system VARCHAR(50),
    created_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT fk_employee_payroll FOREIGN KEY (employee_id) 
        REFERENCES silver.employee_master(employee_id)
);

-- Gold Layer: Business-ready analytics tables
CREATE TABLE gold.workforce_analytics (
    analytics_date DATE,
    department NVARCHAR(100),
    location_code VARCHAR(20),
    headcount INT,
    new_hires INT,
    terminations INT,
    avg_tenure_months DECIMAL(10,2),
    avg_salary DECIMAL(18,2),
    turnover_rate DECIMAL(5,2),
    diversity_score DECIMAL(3,2),
    engagement_score DECIMAL(3,2),
    created_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT pk_workforce_analytics PRIMARY KEY (analytics_date, department, location_code)
);

-- Data Quality View
CREATE VIEW gold.vw_data_quality_dashboard AS
SELECT 
    source_system,
    COUNT(*) as total_records,
    SUM(CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END) as missing_email,
    SUM(CASE WHEN hire_date IS NULL THEN 1 ELSE 0 END) as missing_hire_date,
    AVG(data_quality_score) as avg_quality_score,
    MAX(modified_date) as last_updated
FROM silver.employee_master
GROUP BY source_system;

-- Compliance Report: POPIA/GDPR Data Access Log
CREATE TABLE gold.data_access_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    user_id VARCHAR(100),
    employee_id_accessed VARCHAR(50),
    access_timestamp DATETIME2,
    access_type VARCHAR(50), -- READ, UPDATE, DELETE
    data_fields_accessed NVARCHAR(500),
    purpose VARCHAR(200),
    ip_address VARCHAR(45),
    CONSTRAINT fk_employee_access FOREIGN KEY (employee_id_accessed) 
        REFERENCES silver.employee_master(employee_id)
);`,
      python: `# HR Data Platform - Python Automation & Orchestration
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import logging
from datetime import datetime, timedelta
import hashlib
import json

class HRDataPlatform:
    """Enterprise HR Data Platform - ETL Orchestration"""
    
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.blob_client = BlobServiceClient(
            account_url="https://hrdatalake.blob.core.windows.net",
            credential=self.credential
        )
        self.sql_conn = self.get_sql_connection()
        self.logger = self.setup_logging()
    
    def get_sql_connection(self):
        """Connect to Azure SQL Database"""
        conn_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=tcp:hr-data-platform.database.windows.net,1433;"
            "Database=HRDataWarehouse;"
            "Authentication=ActiveDirectoryMsi;"
        )
        return pyodbc.connect(conn_string)
    
    def extract_from_successfactors(self, extract_date):
        """Extract employee data from SAP SuccessFactors API"""
        self.logger.info(f"Extracting SuccessFactors data for {extract_date}")
        
        # API call to SuccessFactors OData API
        api_url = "https://api.successfactors.com/odata/v2/User"
        headers = {"Authorization": f"Bearer {self.get_sf_token()}"}
        
        response = requests.get(api_url, headers=headers, params={
            "$filter": f"lastModifiedDateTime ge datetime'{extract_date}T00:00:00'",
            "$select": "userId,firstName,lastName,email,hireDate,department,jobTitle"
        })
        
        employees = response.json()['d']['results']
        
        # Store raw data in Bronze layer (Blob Storage)
        self.store_bronze_data(employees, 'successfactors', extract_date)
        
        return pd.DataFrame(employees)
    
    def store_bronze_data(self, data, source_system, extract_date):
        """Store raw data in Bronze layer (Azure Blob Storage)"""
        container_client = self.blob_client.get_container_client("bronze")
        
        blob_path = f"{source_system}/{extract_date}/employees.json"
        blob_client = container_client.get_blob_client(blob_path)
        
        blob_client.upload_blob(
            json.dumps(data, default=str),
            overwrite=True,
            metadata={
                'source_system': source_system,
                'extract_date': extract_date,
                'record_count': str(len(data))
            }
        )
        
        self.logger.info(f"Stored {len(data)} records in Bronze: {blob_path}")
    
    def transform_to_silver(self, df, source_system):
        """Transform and standardize data for Silver layer"""
        self.logger.info(f"Transforming {source_system} data to Silver layer")
        
        # Data cleaning and standardization
        df_clean = df.copy()
        
        # Standardize column names
        column_mapping = {
            'userId': 'employee_id',
            'firstName': 'first_name',
            'lastName': 'last_name',
            'hireDate': 'hire_date',
            'jobTitle': 'job_title'
        }
        df_clean = df_clean.rename(columns=column_mapping)
        
        # Data quality checks
        df_clean['email'] = df_clean['email'].str.lower().str.strip()
        df_clean['data_quality_score'] = self.calculate_quality_score(df_clean)
        
        # Handle missing values
        df_clean['department'] = df_clean['department'].fillna('Unknown')
        df_clean['employment_status'] = 'Active'
        
        # Add audit columns
        df_clean['source_system'] = source_system
        df_clean['created_date'] = datetime.now()
        df_clean['modified_date'] = datetime.now()
        df_clean['is_active'] = 1
        
        # PII encryption for sensitive fields
        df_clean['email_hash'] = df_clean['email'].apply(
            lambda x: hashlib.sha256(x.encode()).hexdigest()
        )
        
        return df_clean
    
    def calculate_quality_score(self, df):
        """Calculate data quality score for each record"""
        required_fields = ['employee_id', 'first_name', 'last_name', 'email', 'hire_date']
        
        quality_scores = []
        for _, row in df.iterrows():
            score = 0.0
            for field in required_fields:
                if pd.notna(row.get(field)) and row.get(field) != '':
                    score += 0.2
            
            # Email validation
            if pd.notna(row.get('email')) and '@' in str(row.get('email')):
                score += 0.1
            
            # Date validation
            if pd.notna(row.get('hire_date')):
                try:
                    hire_date = pd.to_datetime(row.get('hire_date'))
                    if hire_date < datetime.now():
                        score += 0.1
                except:
                    pass
            
            quality_scores.append(min(score, 1.0))
        
        return quality_scores
    
    def load_to_silver(self, df):
        """Load transformed data to Silver layer (Azure SQL)"""
        self.logger.info(f"Loading {len(df)} records to Silver layer")
        
        cursor = self.sql_conn.cursor()
        
        # Upsert logic (merge)
        for _, row in df.iterrows():
            cursor.execute("""
                MERGE silver.employee_master AS target
                USING (SELECT ? AS employee_id) AS source
                ON target.employee_id = source.employee_id
                WHEN MATCHED THEN
                    UPDATE SET
                        first_name = ?,
                        last_name = ?,
                        email = ?,
                        hire_date = ?,
                        job_title = ?,
                        department = ?,
                        modified_date = GETDATE(),
                        data_quality_score = ?
                WHEN NOT MATCHED THEN
                    INSERT (employee_id, first_name, last_name, email, hire_date, 
                            job_title, department, source_system, data_quality_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                row['employee_id'],
                row['first_name'], row['last_name'], row['email'],
                row['hire_date'], row['job_title'], row['department'],
                row['data_quality_score'],
                row['employee_id'], row['first_name'], row['last_name'],
                row['email'], row['hire_date'], row['job_title'],
                row['department'], row['source_system'], row['data_quality_score']
            ))
        
        self.sql_conn.commit()
        self.logger.info("Silver layer load completed")
    
    def build_gold_analytics(self):
        """Build Gold layer analytics tables"""
        self.logger.info("Building Gold layer analytics")
        
        cursor = self.sql_conn.cursor()
        
        # Workforce analytics aggregation
        cursor.execute("""
            INSERT INTO gold.workforce_analytics
            SELECT 
                CAST(GETDATE() AS DATE) as analytics_date,
                department,
                location_code,
                COUNT(*) as headcount,
                SUM(CASE WHEN hire_date >= DATEADD(month, -1, GETDATE()) THEN 1 ELSE 0 END) as new_hires,
                SUM(CASE WHEN termination_date >= DATEADD(month, -1, GETDATE()) THEN 1 ELSE 0 END) as terminations,
                AVG(DATEDIFF(month, hire_date, COALESCE(termination_date, GETDATE()))) as avg_tenure_months,
                0 as avg_salary, -- Calculated separately from payroll
                CAST(SUM(CASE WHEN termination_date >= DATEADD(month, -1, GETDATE()) THEN 1 ELSE 0 END) AS FLOAT) / 
                    NULLIF(COUNT(*), 0) * 100 as turnover_rate,
                0 as diversity_score,
                0 as engagement_score,
                GETDATE() as created_date
            FROM silver.employee_master
            WHERE is_active = 1
            GROUP BY department, location_code;
        """)
        
        self.sql_conn.commit()
        self.logger.info("Gold layer analytics completed")
    
    def run_daily_pipeline(self):
        """Execute daily ETL pipeline"""
        extract_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            # Extract from multiple sources
            sf_data = self.extract_from_successfactors(extract_date)
            
            # Transform to Silver
            silver_data = self.transform_to_silver(sf_data, 'successfactors')
            
            # Load to Silver
            self.load_to_silver(silver_data)
            
            # Build Gold analytics
            self.build_gold_analytics()
            
            self.logger.info("Daily pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

# Azure Function trigger for daily execution
def main(mytimer):
    platform = HRDataPlatform()
    platform.run_daily_pipeline()`,
      alteryx: `<!-- Alteryx Workflow XML - HR Data Integration -->
<!-- This represents an Alteryx workflow for HR data processing -->

Alteryx Workflow: HR_Data_Integration_Master.yxmd

WORKFLOW COMPONENTS:

1. INPUT TOOLS:
   - Input Data (1): SAP SuccessFactors ODBC Connection
     * Connection: DSN=SuccessFactors_Prod
     * Query: SELECT * FROM Employee WHERE LastModified >= DATEADD(day, -1, GETDATE())
   
   - Input Data (2): Workday REST API
     * URL: https://api.workday.com/v1/employees
     * Authentication: OAuth 2.0
     * Headers: Authorization Bearer Token
   
   - Input Data (3): ADP Payroll CSV Files
     * Directory: \\\\fileserver\\payroll\\exports\\
     * File Pattern: ADP_Payroll_*.csv
     * Wildcard: Use * for multiple files

2. DATA PREPARATION:
   - Select Tool: Standardize column names across sources
     * Rename: EmpID → employee_id
     * Rename: FirstName → first_name
     * Rename: LastName → last_name
     * Rename: EmailAddress → email
   
   - Data Cleansing Tool:
     * Remove leading/trailing spaces
     * Convert email to lowercase
     * Replace NULL with 'Unknown' for department
     * Remove special characters from employee_id
   
   - Formula Tool: Calculate data quality score
     * Expression: 
       IF IsNull([email]) THEN 0.0
       ELSEIF Contains([email], '@') THEN 1.0
       ELSE 0.5
       ENDIF

3. DATA QUALITY CHECKS:
   - Filter Tool: Separate valid vs invalid records
     * Valid: data_quality_score >= 0.7
     * Invalid: data_quality_score < 0.7
   
   - Message Tool: Alert on data quality issues
     * Condition: [Invalid_Records] > 100
     * Message: "High number of invalid records detected"
     * Priority: High

4. DATA TRANSFORMATION:
   - Join Tool: Merge employee and payroll data
     * Join Type: Left Outer
     * Join On: employee_id = employee_id
     * Select: All fields from Employee, Salary fields from Payroll
   
   - Summarize Tool: Calculate department metrics
     * Group By: department, location
     * Sum: headcount
     * Average: salary, tenure_months
     * Count: new_hires, terminations

5. COMPLIANCE & GOVERNANCE:
   - Formula Tool: PII Masking
     * Expression: Left([email], 3) + '***@' + Right([email], 10)
     * Apply to: email field for non-authorized users
   
   - Append Fields: Add audit columns
     * source_system = 'Alteryx'
     * load_timestamp = DateTimeNow()
     * batch_id = [Workflow_Name] + '_' + ToString(DateTimeNow())

6. OUTPUT TOOLS:
   - Output Data (1): Azure SQL - Bronze Layer
     * Connection: Azure SQL Database
     * Table: bronze.employee_raw
     * Mode: Append
     * Pre-SQL: TRUNCATE TABLE bronze.employee_raw_staging
   
   - Output Data (2): Azure SQL - Silver Layer
     * Connection: Azure SQL Database
     * Table: silver.employee_master
     * Mode: Update/Insert (Upsert)
     * Key Field: employee_id
   
   - Output Data (3): Azure Blob Storage
     * Connection: Azure Blob Storage
     * Container: hr-data-archive
     * File: employee_extract_[YYYY-MM-DD].parquet
     * Format: Parquet (compressed)

7. WORKFLOW SCHEDULING:
   - Alteryx Server Schedule:
     * Frequency: Daily at 2:00 AM
     * Retry: 3 attempts with 15-minute intervals
     * Notification: Email on failure
     * Priority: High

8. ERROR HANDLING:
   - Test Tool: Validate data before output
     * Test: COUNT([employee_id]) > 0
     * Test: MAX([data_quality_score]) >= 0.7
   
   - Email Tool: Send failure notifications
     * To: data-engineering@company.com
     * Subject: HR Data Pipeline Failed
     * Body: Include error details and record counts

WORKFLOW PERFORMANCE:
- Average Runtime: 12 minutes
- Records Processed: ~50,000 employees
- Data Sources: 8 HRIS systems
- Output Tables: 3 (Bronze, Silver, Gold)`,
      powerbi: `// Power BI DAX Measures - HR Analytics Dashboard

// ============================================
// WORKFORCE METRICS
// ============================================

Total Headcount = 
CALCULATE(
    COUNTROWS('Employee'),
    'Employee'[employment_status] = "Active"
)

Headcount Previous Month = 
CALCULATE(
    [Total Headcount],
    DATEADD('Date'[Date], -1, MONTH)
)

Headcount Change = 
[Total Headcount] - [Headcount Previous Month]

Headcount Change % = 
DIVIDE(
    [Headcount Change],
    [Headcount Previous Month],
    0
)

// ============================================
// TURNOVER METRICS
// ============================================

Terminations MTD = 
CALCULATE(
    COUNTROWS('Employee'),
    'Employee'[termination_date] >= STARTOFMONTH('Date'[Date]),
    'Employee'[termination_date] <= ENDOFMONTH('Date'[Date])
)

Turnover Rate = 
VAR AvgHeadcount = 
    CALCULATE(
        AVERAGE('Workforce Analytics'[headcount]),
        DATESINPERIOD('Date'[Date], MAX('Date'[Date]), -12, MONTH)
    )
VAR Terminations = 
    CALCULATE(
        SUM('Workforce Analytics'[terminations]),
        DATESINPERIOD('Date'[Date], MAX('Date'[Date]), -12, MONTH)
    )
RETURN
    DIVIDE(Terminations, AvgHeadcount, 0) * 100

Voluntary Turnover Rate = 
CALCULATE(
    [Turnover Rate],
    'Employee'[termination_reason] IN {"Resignation", "Retirement"}
)

// ============================================
// HIRING METRICS
// ============================================

New Hires MTD = 
CALCULATE(
    COUNTROWS('Employee'),
    'Employee'[hire_date] >= STARTOFMONTH('Date'[Date]),
    'Employee'[hire_date] <= ENDOFMONTH('Date'[Date])
)

Time to Fill (Days) = 
AVERAGE(
    DATEDIFF(
        'Requisition'[posted_date],
        'Requisition'[filled_date],
        DAY
    )
)

Hiring Velocity = 
DIVIDE(
    [New Hires MTD],
    DISTINCTCOUNT('Requisition'[requisition_id]),
    0
)

// ============================================
// COMPENSATION METRICS
// ============================================

Total Compensation = 
SUMX(
    'Employee',
    'Employee'[base_salary] + 
    'Employee'[bonus] + 
    'Employee'[benefits_value]
)

Average Salary by Department = 
CALCULATE(
    AVERAGE('Employee'[base_salary]),
    ALLEXCEPT('Employee', 'Employee'[department])
)

Compensation Ratio = 
DIVIDE(
    'Employee'[base_salary],
    [Average Salary by Department],
    0
)

Salary Budget Variance = 
VAR ActualSalary = [Total Compensation]
VAR BudgetedSalary = SUM('Budget'[salary_budget])
RETURN
    ActualSalary - BudgetedSalary

// ============================================
// DIVERSITY METRICS
// ============================================

Gender Diversity % = 
VAR FemaleCount = 
    CALCULATE(
        COUNTROWS('Employee'),
        'Employee'[gender] = "Female"
    )
VAR TotalCount = [Total Headcount]
RETURN
    DIVIDE(FemaleCount, TotalCount, 0) * 100

Diversity Index = 
VAR Categories = 
    DISTINCTCOUNT('Employee'[diversity_category])
VAR MaxCategories = 10
RETURN
    DIVIDE(Categories, MaxCategories, 0) * 100

// ============================================
// TENURE & RETENTION
// ============================================

Average Tenure (Years) = 
AVERAGEX(
    'Employee',
    DATEDIFF(
        'Employee'[hire_date],
        IF(
            ISBLANK('Employee'[termination_date]),
            TODAY(),
            'Employee'[termination_date]
        ),
        DAY
    ) / 365.25
)

Retention Rate = 
VAR StartHeadcount = [Headcount Previous Month]
VAR EndHeadcount = [Total Headcount]
VAR Terminations = [Terminations MTD]
RETURN
    DIVIDE(
        EndHeadcount,
        StartHeadcount + [New Hires MTD],
        0
    ) * 100

// ============================================
// COMPLIANCE METRICS
// ============================================

Data Quality Score = 
AVERAGE('Employee'[data_quality_score]) * 100

Records Missing Critical Data = 
CALCULATE(
    COUNTROWS('Employee'),
    OR(
        ISBLANK('Employee'[email]),
        ISBLANK('Employee'[hire_date])
    )
)

POPIA Compliance % = 
VAR TotalRecords = COUNTROWS('Employee')
VAR CompliantRecords = 
    CALCULATE(
        COUNTROWS('Employee'),
        'Employee'[consent_obtained] = TRUE,
        'Employee'[data_classification] <> BLANK()
    )
RETURN
    DIVIDE(CompliantRecords, TotalRecords, 0) * 100

// ============================================
// PREDICTIVE ANALYTICS
// ============================================

Flight Risk Score = 
VAR TenureScore = 
    IF([Average Tenure (Years)] < 2, 0.3, 0)
VAR EngagementScore = 
    IF('Employee'[engagement_score] < 3, 0.4, 0)
VAR SalaryScore = 
    IF([Compensation Ratio] < 0.9, 0.3, 0)
RETURN
    TenureScore + EngagementScore + SalaryScore

Employees at Risk = 
CALCULATE(
    COUNTROWS('Employee'),
    [Flight Risk Score] >= 0.6
)`,
    },
  },
  "insurance-analytics": {
    title: "Insurance Analytics & Insights Platform",
    problemStatement:
      "The Insurance & Asset Management division struggled with fragmented data across policy administration, claims processing, and customer interaction systems. Business units couldn't access timely insights on policy performance, claims trends, or customer behavior, hindering strategic decision-making. Manual reporting processes took 5-7 days, preventing proactive risk management and personalized customer engagement. The platform needed to handle unstructured data from multiple sources and provide real-time analytics for operational and strategic decisions.",
    architecture: "/insurance-analytics-platform-architecture-showing-d.jpg",
    solution: {
      sql: `-- Insurance Analytics Platform - SQL Data Models
-- Policy Performance Analytics
CREATE VIEW analytics.vw_policy_performance AS
WITH policy_metrics AS (
  SELECT 
    p.policy_id,
    p.policy_number,
    p.product_type,
    p.coverage_type,
    p.policy_start_date,
    p.policy_end_date,
    p.annual_premium,
    p.sum_insured,
    c.customer_id,
    c.customer_segment,
    c.risk_profile,
    
    -- Claims metrics
    COUNT(cl.claim_id) as total_claims,
    SUM(cl.claim_amount) as total_claim_amount,
    AVG(cl.claim_amount) as avg_claim_amount,
    SUM(CASE WHEN cl.claim_status = 'Approved' THEN cl.claim_amount ELSE 0 END) as paid_claims,
    
    -- Loss ratio calculation
    CASE WHEN p.annual_premium > 0 
         THEN (SUM(cl.claim_amount) / p.annual_premium) * 100 
         ELSE 0 END as loss_ratio,
    
    -- Policy tenure
    DATEDIFF(month, p.policy_start_date, COALESCE(p.policy_end_date, GETDATE())) as policy_tenure_months,
    
    -- Renewal indicator
    CASE WHEN p.renewal_date IS NOT NULL THEN 1 ELSE 0 END as is_renewed,
    
    -- Customer lifetime value
    p.annual_premium * DATEDIFF(year, p.policy_start_date, COALESCE(p.policy_end_date, GETDATE())) as customer_ltv
    
  FROM policies p
  INNER JOIN customers c ON p.customer_id = c.customer_id
  LEFT JOIN claims cl ON p.policy_id = cl.policy_id
  WHERE p.policy_start_date >= '2020-01-01'
  GROUP BY 
    p.policy_id, p.policy_number, p.product_type, p.coverage_type,
    p.policy_start_date, p.policy_end_date, p.annual_premium, p.sum_insured,
    c.customer_id, c.customer_segment, c.risk_profile, p.renewal_date
)
SELECT 
  *,
  -- Risk categorization
  CASE 
    WHEN loss_ratio > 100 THEN 'High Risk'
    WHEN loss_ratio > 70 THEN 'Medium Risk'
    WHEN loss_ratio > 40 THEN 'Low Risk'
    ELSE 'Very Low Risk'
  END as risk_category,
  
  -- Profitability score
  (annual_premium - total_claim_amount) as underwriting_profit,
  
  -- Churn prediction indicator
  CASE 
    WHEN policy_tenure_months < 12 AND total_claims > 2 THEN 'High Churn Risk'
    WHEN policy_tenure_months < 24 AND loss_ratio > 80 THEN 'Medium Churn Risk'
    ELSE 'Low Churn Risk'
  END as churn_risk
FROM policy_metrics;

-- Claims Analytics with Root Cause Analysis
CREATE PROCEDURE analytics.sp_claims_root_cause_analysis
    @start_date DATE,
    @end_date DATE,
    @product_type VARCHAR(50) = NULL
AS
BEGIN
    -- Claims frequency and severity analysis
    WITH claims_analysis AS (
        SELECT 
            c.claim_id,
            c.claim_number,
            c.claim_date,
            c.claim_amount,
            c.claim_type,
            c.claim_cause,
            c.settlement_days,
            p.product_type,
            p.coverage_type,
            cust.customer_segment,
            cust.age_group,
            cust.geographic_region,
            
            -- Time-based features
            DATEPART(month, c.claim_date) as claim_month,
            DATEPART(quarter, c.claim_date) as claim_quarter,
            DATEPART(weekday, c.claim_date) as claim_day_of_week,
            
            -- Claim severity classification
            CASE 
                WHEN c.claim_amount > 100000 THEN 'Catastrophic'
                WHEN c.claim_amount > 50000 THEN 'Major'
                WHEN c.claim_amount > 10000 THEN 'Moderate'
                ELSE 'Minor'
            END as severity_level
            
        FROM claims c
        INNER JOIN policies p ON c.policy_id = p.policy_id
        INNER JOIN customers cust ON p.customer_id = cust.customer_id
        WHERE c.claim_date BETWEEN @start_date AND @end_date
          AND (@product_type IS NULL OR p.product_type = @product_type)
    ),
    
    root_cause_summary AS (
        SELECT 
            claim_cause,
            product_type,
            COUNT(*) as claim_count,
            SUM(claim_amount) as total_claim_amount,
            AVG(claim_amount) as avg_claim_amount,
            AVG(settlement_days) as avg_settlement_days,
            
            -- Distribution by severity
            SUM(CASE WHEN severity_level = 'Catastrophic' THEN 1 ELSE 0 END) as catastrophic_count,
            SUM(CASE WHEN severity_level = 'Major' THEN 1 ELSE 0 END) as major_count,
            SUM(CASE WHEN severity_level = 'Moderate' THEN 1 ELSE 0 END) as moderate_count,
            SUM(CASE WHEN severity_level = 'Minor' THEN 1 ELSE 0 END) as minor_count,
            
            -- Geographic concentration
            COUNT(DISTINCT geographic_region) as affected_regions,
            
            -- Trend indicators
            COUNT(CASE WHEN claim_month IN (1,2,3) THEN 1 END) as q1_claims,
            COUNT(CASE WHEN claim_month IN (4,5,6) THEN 1 END) as q2_claims,
            COUNT(CASE WHEN claim_month IN (7,8,9) THEN 1 END) as q3_claims,
            COUNT(CASE WHEN claim_month IN (10,11,12) THEN 1 END) as q4_claims
            
        FROM claims_analysis
        GROUP BY claim_cause, product_type
    )
    
    SELECT 
        *,
        -- Impact score (frequency * severity)
        (claim_count * avg_claim_amount / 1000) as impact_score,
        
        -- Percentage of total claims
        CAST(claim_count AS FLOAT) / SUM(claim_count) OVER() * 100 as pct_of_total_claims,
        
        -- Cumulative percentage (Pareto analysis)
        SUM(CAST(claim_count AS FLOAT) / SUM(claim_count) OVER() * 100) 
            OVER(ORDER BY claim_count DESC) as cumulative_pct
            
    FROM root_cause_summary
    ORDER BY impact_score DESC;
END;

-- Customer Insights & Segmentation
CREATE VIEW analytics.vw_customer_360 AS
WITH customer_policies AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT policy_id) as total_policies,
        SUM(annual_premium) as total_annual_premium,
        MIN(policy_start_date) as first_policy_date,
        MAX(policy_start_date) as latest_policy_date,
        COUNT(CASE WHEN policy_status = 'Active' THEN 1 END) as active_policies,
        STRING_AGG(product_type, ', ') as product_mix
    FROM policies
    GROUP BY customer_id
),
customer_claims AS (
    SELECT 
        p.customer_id,
        COUNT(c.claim_id) as lifetime_claims,
        SUM(c.claim_amount) as lifetime_claim_amount,
        MAX(c.claim_date) as last_claim_date,
        AVG(c.settlement_days) as avg_settlement_days
    FROM claims c
    INNER JOIN policies p ON c.policy_id = p.policy_id
    GROUP BY p.customer_id
),
customer_interactions AS (
    SELECT 
        customer_id,
        COUNT(*) as total_interactions,
        SUM(CASE WHEN interaction_type = 'Complaint' THEN 1 ELSE 0 END) as complaint_count,
        SUM(CASE WHEN interaction_type = 'Inquiry' THEN 1 ELSE 0 END) as inquiry_count,
        AVG(satisfaction_score) as avg_satisfaction_score
    FROM customer_interactions
    WHERE interaction_date >= DATEADD(year, -1, GETDATE())
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_name,
    c.age_group,
    c.income_bracket,
    c.geographic_region,
    c.customer_since_date,
    
    -- Policy metrics
    cp.total_policies,
    cp.active_policies,
    cp.total_annual_premium,
    cp.product_mix,
    DATEDIFF(year, cp.first_policy_date, GETDATE()) as customer_tenure_years,
    
    -- Claims behavior
    COALESCE(cc.lifetime_claims, 0) as lifetime_claims,
    COALESCE(cc.lifetime_claim_amount, 0) as lifetime_claim_amount,
    CASE WHEN cp.total_annual_premium > 0 
         THEN (COALESCE(cc.lifetime_claim_amount, 0) / cp.total_annual_premium) * 100 
         ELSE 0 END as customer_loss_ratio,
    
    -- Engagement metrics
    COALESCE(ci.total_interactions, 0) as annual_interactions,
    COALESCE(ci.complaint_count, 0) as annual_complaints,
    COALESCE(ci.avg_satisfaction_score, 0) as satisfaction_score,
    
    -- Customer value segmentation
    CASE 
        WHEN cp.total_annual_premium > 50000 AND COALESCE(cc.lifetime_claims, 0) < 2 THEN 'Premium Low Risk'
        WHEN cp.total_annual_premium > 50000 THEN 'Premium High Value'
        WHEN cp.total_annual_premium > 20000 AND COALESCE(cc.lifetime_claims, 0) < 3 THEN 'Standard Profitable'
        WHEN cp.total_annual_premium > 20000 THEN 'Standard Monitor'
        WHEN COALESCE(cc.lifetime_claims, 0) > 5 THEN 'High Risk'
        ELSE 'Basic'
    END as customer_segment,
    
    -- Churn risk score (0-100)
    (
        CASE WHEN DATEDIFF(day, cc.last_claim_date, GETDATE()) < 90 THEN 20 ELSE 0 END +
        CASE WHEN ci.complaint_count > 2 THEN 30 ELSE 0 END +
        CASE WHEN ci.avg_satisfaction_score < 3 THEN 25 ELSE 0 END +
        CASE WHEN cp.active_policies = 0 THEN 25 ELSE 0 END
    ) as churn_risk_score
    
FROM customers c
LEFT JOIN customer_policies cp ON c.customer_id = cp.customer_id
LEFT JOIN customer_claims cc ON c.customer_id = cc.customer_id
LEFT JOIN customer_interactions ci ON c.customer_id = ci.customer_id;

-- Real Estate Insurance Analytics
CREATE VIEW analytics.vw_real_estate_insurance_insights AS
SELECT 
    p.policy_id,
    p.policy_number,
    p.property_type,
    p.property_location,
    p.property_value,
    p.coverage_amount,
    p.annual_premium,
    
    -- Property risk factors
    pr.building_age,
    pr.construction_type,
    pr.security_features_score,
    pr.natural_disaster_zone,
    pr.crime_rate_index,
    
    -- Claims history
    COUNT(c.claim_id) as property_claims_count,
    SUM(c.claim_amount) as total_claims_amount,
    
    -- Risk assessment
    CASE 
        WHEN pr.building_age > 50 THEN 'High Age Risk'
        WHEN pr.building_age > 30 THEN 'Medium Age Risk'
        ELSE 'Low Age Risk'
    END as age_risk_category,
    
    CASE 
        WHEN pr.natural_disaster_zone IN ('Flood', 'Earthquake') THEN 'High Natural Risk'
        WHEN pr.natural_disaster_zone IN ('Storm', 'Wildfire') THEN 'Medium Natural Risk'
        ELSE 'Low Natural Risk'
    END as natural_risk_category,
    
    -- Premium adequacy
    (p.annual_premium / p.coverage_amount) * 100 as premium_rate_pct,
    CASE 
        WHEN (SUM(c.claim_amount) / p.annual_premium) > 1.2 THEN 'Underpriced'
        WHEN (SUM(c.claim_amount) / p.annual_premium) < 0.3 THEN 'Overpriced'
        ELSE 'Adequately Priced'
    END as pricing_assessment
    
FROM policies p
INNER JOIN property_details pr ON p.policy_id = pr.policy_id
LEFT JOIN claims c ON p.policy_id = c.policy_id
WHERE p.product_type = 'Property Insurance'
GROUP BY 
    p.policy_id, p.policy_number, p.property_type, p.property_location,
    p.property_value, p.coverage_amount, p.annual_premium,
    pr.building_age, pr.construction_type, pr.security_features_score,
    pr.natural_disaster_zone, pr.crime_rate_index;`,
      python: `# Insurance Analytics Platform - Python Data Processing
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class InsuranceAnalyticsPlatform:
    """
    Enterprise Insurance Analytics & Insights Platform
    Handles policy performance, claims analytics, and customer insights
    """
    
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.scaler = StandardScaler()
    
    def extract_policy_data(self, start_date, end_date):
        """Extract policy and claims data for analysis"""
        query = """
        SELECT 
            p.policy_id,
            p.policy_number,
            p.product_type,
            p.annual_premium,
            p.sum_insured,
            p.policy_start_date,
            c.customer_id,
            c.age_group,
            c.customer_segment,
            c.geographic_region,
            COUNT(cl.claim_id) as claim_count,
            SUM(cl.claim_amount) as total_claim_amount,
            DATEDIFF(month, p.policy_start_date, GETDATE()) as policy_age_months
        FROM policies p
        INNER JOIN customers c ON p.customer_id = c.customer_id
        LEFT JOIN claims cl ON p.policy_id = cl.policy_id
        WHERE p.policy_start_date BETWEEN ? AND ?
        GROUP BY 
            p.policy_id, p.policy_number, p.product_type, p.annual_premium,
            p.sum_insured, p.policy_start_date, c.customer_id, c.age_group,
            c.customer_segment, c.geographic_region
        """
        
        df = pd.read_sql(query, self.conn, params=[start_date, end_date])
        return df
    
    def calculate_loss_ratios(self, df):
        """Calculate loss ratios and risk metrics"""
        df['loss_ratio'] = np.where(
            df['annual_premium'] > 0,
            (df['total_claim_amount'] / df['annual_premium']) * 100,
            0
        )
        
        df['risk_category'] = pd.cut(
            df['loss_ratio'],
            bins=[0, 40, 70, 100, float('inf')],
            labels=['Very Low Risk', 'Low Risk', 'Medium Risk', 'High Risk']
        )
        
        df['underwriting_profit'] = df['annual_premium'] - df['total_claim_amount']
        
        return df
    
    def churn_prediction_model(self, df):
        """Build churn prediction model using Random Forest"""
        # Feature engineering
        features = df[['annual_premium', 'sum_insured', 'claim_count', 
                      'total_claim_amount', 'policy_age_months']].copy()
        
        # Create target variable (churn indicator)
        # Assuming we have renewal data
        df['churned'] = np.where(
            (df['policy_age_months'] > 12) & (df['claim_count'] > 2) & (df['loss_ratio'] > 80),
            1, 0
        )
        
        # Handle missing values
        features = features.fillna(features.median())
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            features, df['churned'], test_size=0.3, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'
        )
        model.fit(X_train_scaled, y_train)
        
        # Predictions
        df['churn_probability'] = model.predict_proba(
            self.scaler.transform(features)
        )[:, 1]
        
        df['churn_risk_segment'] = pd.cut(
            df['churn_probability'],
            bins=[0, 0.3, 0.6, 1.0],
            labels=['Low Churn Risk', 'Medium Churn Risk', 'High Churn Risk']
        )
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': features.columns,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        return df, model, feature_importance
    
    def claims_root_cause_analysis(self, start_date, end_date):
        """Perform root cause analysis on claims data"""
        query = """
        SELECT 
            c.claim_id,
            c.claim_date,
            c.claim_amount,
            c.claim_type,
            c.claim_cause,
            c.settlement_days,
            p.product_type,
            p.coverage_type,
            cust.geographic_region,
            DATEPART(month, c.claim_date) as claim_month,
            DATEPART(quarter, c.claim_date) as claim_quarter
        FROM claims c
        INNER JOIN policies p ON c.policy_id = p.policy_id
        INNER JOIN customers cust ON p.customer_id = cust.customer_id
        WHERE c.claim_date BETWEEN ? AND ?
        """
        
        claims_df = pd.read_sql(query, self.conn, params=[start_date, end_date])
        
        # Severity classification
        claims_df['severity_level'] = pd.cut(
            claims_df['claim_amount'],
            bins=[0, 10000, 50000, 100000, float('inf')],
            labels=['Minor', 'Moderate', 'Major', 'Catastrophic']
        )
        
        # Root cause summary
        root_cause_summary = claims_df.groupby(['claim_cause', 'product_type']).agg({
            'claim_id': 'count',
            'claim_amount': ['sum', 'mean'],
            'settlement_days': 'mean'
        }).reset_index()
        
        root_cause_summary.columns = ['claim_cause', 'product_type', 'claim_count', 
                                      'total_amount', 'avg_amount', 'avg_settlement_days']
        
        # Calculate impact score
        root_cause_summary['impact_score'] = (
            root_cause_summary['claim_count'] * 
            root_cause_summary['avg_amount'] / 1000
        )
        
        # Pareto analysis
        root_cause_summary = root_cause_summary.sort_values('impact_score', ascending=False)
        root_cause_summary['cumulative_pct'] = (
            root_cause_summary['claim_count'].cumsum() / 
            root_cause_summary['claim_count'].sum() * 100
        )
        
        return claims_df, root_cause_summary
    
    def customer_segmentation_analysis(self):
        """Perform customer segmentation using RFM analysis"""
        query = """
        SELECT 
            c.customer_id,
            c.customer_name,
            c.customer_segment,
            COUNT(DISTINCT p.policy_id) as total_policies,
            SUM(p.annual_premium) as total_premium,
            MAX(p.policy_start_date) as last_policy_date,
            COUNT(cl.claim_id) as total_claims,
            SUM(cl.claim_amount) as total_claim_amount
        FROM customers c
        LEFT JOIN policies p ON c.customer_id = p.customer_id
        LEFT JOIN claims cl ON p.policy_id = cl.policy_id
        GROUP BY c.customer_id, c.customer_name, c.customer_segment
        """
        
        customers_df = pd.read_sql(query, self.conn)
        
        # RFM Analysis
        customers_df['recency_days'] = (
            datetime.now() - pd.to_datetime(customers_df['last_policy_date'])
        ).dt.days
        
        # Calculate RFM scores (1-5 scale)
        customers_df['recency_score'] = pd.qcut(
            customers_df['recency_days'], 
            q=5, 
            labels=[5, 4, 3, 2, 1],
            duplicates='drop'
        )
        
        customers_df['frequency_score'] = pd.qcut(
            customers_df['total_policies'], 
            q=5, 
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        )
        
        customers_df['monetary_score'] = pd.qcut(
            customers_df['total_premium'], 
            q=5, 
            labels=[1, 2, 3, 4, 5],
            duplicates='drop'
        )
        
        # Calculate RFM segment
        customers_df['rfm_score'] = (
            customers_df['recency_score'].astype(int) +
            customers_df['frequency_score'].astype(int) +
            customers_df['monetary_score'].astype(int)
        )
        
        # Segment customers
        def segment_customer(row):
            if row['rfm_score'] >= 13:
                return 'Champions'
            elif row['rfm_score'] >= 10:
                return 'Loyal Customers'
            elif row['rfm_score'] >= 7:
                return 'Potential Loyalists'
            elif row['rfm_score'] >= 5:
                return 'At Risk'
            else:
                return 'Lost'
        
        customers_df['customer_value_segment'] = customers_df.apply(segment_customer, axis=1)
        
        return customers_df
    
    def generate_executive_dashboard_data(self):
        """Generate data for executive dashboard"""
        # Key metrics
        metrics_query = """
        SELECT 
            COUNT(DISTINCT policy_id) as total_policies,
            SUM(annual_premium) as total_premium_revenue,
            COUNT(DISTINCT customer_id) as total_customers,
            AVG(annual_premium) as avg_premium_per_policy
        FROM policies
        WHERE policy_status = 'Active'
        """
        
        metrics = pd.read_sql(metrics_query, self.conn)
        
        # Claims metrics
        claims_query = """
        SELECT 
            COUNT(*) as total_claims,
            SUM(claim_amount) as total_claim_amount,
            AVG(claim_amount) as avg_claim_amount,
            AVG(settlement_days) as avg_settlement_days
        FROM claims
        WHERE claim_date >= DATEADD(year, -1, GETDATE())
        """
        
        claims_metrics = pd.read_sql(claims_query, self.conn)
        
        # Calculate overall loss ratio
        loss_ratio = (
            claims_metrics['total_claim_amount'].iloc[0] / 
            metrics['total_premium_revenue'].iloc[0] * 100
        )
        
        dashboard_data = {
            'total_policies': metrics['total_policies'].iloc[0],
            'total_premium_revenue': metrics['total_premium_revenue'].iloc[0],
            'total_customers': metrics['total_customers'].iloc[0],
            'avg_premium_per_policy': metrics['avg_premium_per_policy'].iloc[0],
            'total_claims': claims_metrics['total_claims'].iloc[0],
            'total_claim_amount': claims_metrics['total_claim_amount'].iloc[0],
            'avg_claim_amount': claims_metrics['avg_claim_amount'].iloc[0],
            'avg_settlement_days': claims_metrics['avg_settlement_days'].iloc[0],
            'loss_ratio': loss_ratio
        }
        
        return dashboard_data
    
    def export_to_powerbi(self, df, table_name):
        """Export processed data to SQL for Power BI consumption"""
        cursor = self.conn.cursor()
        
        # Create table if not exists
        cursor.execute(f"""
        IF OBJECT_ID('analytics.{table_name}', 'U') IS NOT NULL
            DROP TABLE analytics.{table_name}
        """)
        
        # Use pandas to_sql for efficient bulk insert
        df.to_sql(
            table_name,
            self.conn,
            schema='analytics',
            if_exists='replace',
            index=False
        )
        
        print(f"Data exported to analytics.{table_name} for Power BI")

# Example usage
if __name__ == "__main__":
    conn_string = "Driver={ODBC Driver 18 for SQL Server};Server=insurance-analytics.database.windows.net;Database=InsuranceAnalytics;Authentication=ActiveDirectoryMsi;"
    
    platform = InsuranceAnalyticsPlatform(conn_string)
    
    # Extract and analyze policy data
    start_date = '2023-01-01'
    end_date = '2024-12-31'
    
    policy_data = platform.extract_policy_data(start_date, end_date)
    policy_data = platform.calculate_loss_ratios(policy_data)
    
    # Churn prediction
    policy_data, churn_model, feature_importance = platform.churn_prediction_model(policy_data)
    
    # Claims root cause analysis
    claims_data, root_causes = platform.claims_root_cause_analysis(start_date, end_date)
    
    # Customer segmentation
    customer_segments = platform.customer_segmentation_analysis()
    
    # Generate dashboard data
    dashboard_data = platform.generate_executive_dashboard_data()
    
    # Export to Power BI
    platform.export_to_powerbi(policy_data, 'policy_analytics')
    platform.export_to_powerbi(customer_segments, 'customer_segments')
    platform.export_to_powerbi(root_causes, 'claims_root_causes')
    
    print("Insurance Analytics Platform processing completed!")`,
      powerbi: `// Power BI DAX Measures - Insurance Analytics Dashboard

// === KEY PERFORMANCE INDICATORS ===

// Total Premium Revenue
Total Premium Revenue = 
SUM(policies[annual_premium])

// Total Policies
Total Policies = 
COUNTROWS(policies)

// Total Active Policies
Active Policies = 
CALCULATE(
    COUNTROWS(policies),
    policies[policy_status] = "Active"
)

// Total Claims Amount
Total Claims Amount = 
SUM(claims[claim_amount])

// === LOSS RATIO METRICS ===

// Overall Loss Ratio
Loss Ratio = 
DIVIDE(
    [Total Claims Amount],
    [Total Premium Revenue],
    0
) * 100

// Loss Ratio by Product
Loss Ratio by Product = 
CALCULATE(
    [Loss Ratio],
    ALLEXCEPT(policies, policies[product_type])
)

// Target Loss Ratio (Industry benchmark)
Target Loss Ratio = 70

// Loss Ratio Variance
Loss Ratio Variance = 
[Loss Ratio] - [Target Loss Ratio]

// === CLAIMS ANALYTICS ===

// Average Claim Amount
Avg Claim Amount = 
AVERAGE(claims[claim_amount])

// Claims Frequency
Claims Frequency = 
DIVIDE(
    COUNTROWS(claims),
    [Total Policies],
    0
)

// Average Settlement Days
Avg Settlement Days = 
AVERAGE(claims[settlement_days])

// Claims Pending
Claims Pending = 
CALCULATE(
    COUNTROWS(claims),
    claims[claim_status] = "Pending"
)

// Claims Approval Rate
Claims Approval Rate = 
DIVIDE(
    CALCULATE(COUNTROWS(claims), claims[claim_status] = "Approved"),
    COUNTROWS(claims),
    0
) * 100

// === CUSTOMER ANALYTICS ===

// Total Customers
Total Customers = 
DISTINCTCOUNT(customers[customer_id])

// Customer Lifetime Value
Customer LTV = 
SUMX(
    customers,
    CALCULATE(
        SUM(policies[annual_premium]) * 
        DATEDIFF(
            MIN(policies[policy_start_date]),
            MAX(policies[policy_end_date]),
            YEAR
        )
    )
)

// Average Policies per Customer
Avg Policies per Customer = 
DIVIDE(
    [Total Policies],
    [Total Customers],
    0
)

// Customer Retention Rate
Customer Retention Rate = 
VAR CustomersLastYear = 
    CALCULATE(
        DISTINCTCOUNT(customers[customer_id]),
        DATEADD('Date'[Date], -1, YEAR)
    )
VAR CustomersThisYear = 
    DISTINCTCOUNT(customers[customer_id])
VAR RetainedCustomers = 
    CALCULATE(
        DISTINCTCOUNT(customers[customer_id]),
        FILTER(
            ALL(customers),
            CALCULATE(COUNTROWS(policies), DATEADD('Date'[Date], -1, YEAR)) > 0
        )
    )
RETURN
    DIVIDE(RetainedCustomers, CustomersLastYear, 0) * 100

// === CHURN ANALYTICS ===

// Churn Rate
Churn Rate = 
VAR TotalCustomersStart = 
    CALCULATE(
        DISTINCTCOUNT(customers[customer_id]),
        DATEADD('Date'[Date], -1, YEAR)
    )
VAR ChurnedCustomers = 
    CALCULATE(
        DISTINCTCOUNT(customers[customer_id]),
        customers[churned] = 1
    )
RETURN
    DIVIDE(ChurnedCustomers, TotalCustomersStart, 0) * 100

// High Churn Risk Customers
High Churn Risk Customers = 
CALCULATE(
    DISTINCTCOUNT(customers[customer_id]),
    customers[churn_risk_segment] = "High Churn Risk"
)

// === PROFITABILITY METRICS ===

// Underwriting Profit
Underwriting Profit = 
[Total Premium Revenue] - [Total Claims Amount]

// Profit Margin %
Profit Margin = 
DIVIDE(
    [Underwriting Profit],
    [Total Premium Revenue],
    0
) * 100

// Profitable Policies Count
Profitable Policies = 
CALCULATE(
    COUNTROWS(policies),
    policies[underwriting_profit] > 0
)

// === TIME INTELLIGENCE ===

// Premium Revenue YoY Growth
Premium Revenue YoY Growth = 
VAR CurrentYearRevenue = [Total Premium Revenue]
VAR PreviousYearRevenue = 
    CALCULATE(
        [Total Premium Revenue],
        DATEADD('Date'[Date], -1, YEAR)
    )
RETURN
    DIVIDE(
        CurrentYearRevenue - PreviousYearRevenue,
        PreviousYearRevenue,
        0
    ) * 100

// Claims Amount MoM Change
Claims MoM Change = 
VAR CurrentMonth = [Total Claims Amount]
VAR PreviousMonth = 
    CALCULATE(
        [Total Claims Amount],
        DATEADD('Date'[Date], -1, MONTH)
    )
RETURN
    CurrentMonth - PreviousMonth

// === SEGMENTATION METRICS ===

// Premium Customers Count
Premium Customers = 
CALCULATE(
    DISTINCTCOUNT(customers[customer_id]),
    customers[customer_segment] IN {"Premium Low Risk", "Premium High Value"}
)

// High Risk Policies
High Risk Policies = 
CALCULATE(
    COUNTROWS(policies),
    policies[risk_category] = "High Risk"
)

// === REAL ESTATE SPECIFIC ===

// Property Insurance Premium
Property Premium = 
CALCULATE(
    [Total Premium Revenue],
    policies[product_type] = "Property Insurance"
)

// High Value Properties
High Value Properties = 
CALCULATE(
    COUNTROWS(policies),
    policies[property_value] > 1000000
)

// Natural Disaster Exposure
Natural Disaster Exposure = 
CALCULATE(
    SUM(policies[sum_insured]),
    property_details[natural_disaster_zone] IN {"Flood", "Earthquake", "Storm"}
)

// === CONDITIONAL FORMATTING ===

// Loss Ratio Color
Loss Ratio Color = 
SWITCH(
    TRUE(),
    [Loss Ratio] < 40, "Green",
    [Loss Ratio] < 70, "Yellow",
    [Loss Ratio] < 100, "Orange",
    "Red"
)

// Churn Risk Color
Churn Risk Color = 
SWITCH(
    TRUE(),
    [Churn Rate] < 5, "Green",
    [Churn Rate] < 10, "Yellow",
    "Red"
)
`,
    },
  },
  "credit-lifecycle-bi": {
    title: "Credit Lifecycle BI & Reporting Platform",
    problemStatement:
      "The Business & Commercial Banking division faced significant challenges with manual reporting processes across the credit lifecycle (originations, account management, and collections). Data was scattered across multiple systems including legacy databases, Excel spreadsheets, and disparate HRIS platforms. Executive reports took 5-7 days to compile manually, with frequent data quality issues and inconsistencies. The business lacked real-time visibility into portfolio performance, credit risk trends, and operational efficiency metrics. There was no standardized approach to data access, leading to duplicate efforts and conflicting reports across teams.",
    architecture: "/credit-lifecycle-bi-architecture-showing-sql-server.jpg",
    solution: {
      sql: `-- Credit Portfolio Analytics Data Model
-- Comprehensive view of credit lifecycle performance

-- Originations Performance Analysis
CREATE VIEW vw_originations_performance AS
SELECT 
    o.application_date,
    o.product_type,
    o.customer_segment,
    COUNT(DISTINCT o.application_id) as total_applications,
    SUM(CASE WHEN o.status = 'Approved' THEN 1 ELSE 0 END) as approved_count,
    SUM(CASE WHEN o.status = 'Approved' THEN o.loan_amount ELSE 0 END) as approved_amount,
    CAST(SUM(CASE WHEN o.status = 'Approved' THEN 1 ELSE 0 END) AS FLOAT) / 
        NULLIF(COUNT(*), 0) * 100 as approval_rate,
    AVG(DATEDIFF(day, o.application_date, o.decision_date)) as avg_decision_days
FROM credit_originations o
WHERE o.application_date >= DATEADD(month, -12, GETDATE())
GROUP BY o.application_date, o.product_type, o.customer_segment;

-- Collections Performance with Root Cause Analysis
CREATE PROCEDURE sp_collections_root_cause_analysis
AS
BEGIN
    -- Pareto analysis of delinquency causes
    WITH delinquency_causes AS (
        SELECT 
            c.delinquency_reason,
            COUNT(*) as case_count,
            SUM(c.outstanding_balance) as total_exposure,
            AVG(c.days_past_due) as avg_dpd
        FROM collections_cases c
        WHERE c.status = 'Active'
        GROUP BY c.delinquency_reason
    ),
    ranked_causes AS (
        SELECT 
            *,
            SUM(case_count) OVER (ORDER BY case_count DESC) as running_total,
            SUM(case_count) OVER () as grand_total
        FROM delinquency_causes
    )
    SELECT 
        delinquency_reason,
        case_count,
        total_exposure,
        avg_dpd,
        CAST(case_count AS FLOAT) / grand_total * 100 as pct_of_total,
        CAST(running_total AS FLOAT) / grand_total * 100 as cumulative_pct
    FROM ranked_causes
    ORDER BY case_count DESC;
END;

-- Account Management Performance Metrics
CREATE VIEW vw_account_management_kpis AS
SELECT 
    am.portfolio_manager,
    am.product_line,
    COUNT(DISTINCT am.account_id) as active_accounts,
    SUM(am.current_balance) as total_portfolio_value,
    AVG(am.credit_score) as avg_credit_score,
    SUM(CASE WHEN am.risk_rating = 'High' THEN 1 ELSE 0 END) as high_risk_accounts,
    SUM(CASE WHEN am.days_since_review > 90 THEN 1 ELSE 0 END) as overdue_reviews,
    AVG(am.customer_satisfaction_score) as avg_csat
FROM account_management am
WHERE am.status = 'Active'
GROUP BY am.portfolio_manager, am.product_line;`,

      python: `"""
Credit Lifecycle BI Automation Platform
Automated report generation and data processing
"""

import pandas as pd
import pyodbc
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.chart import BarChart, LineChart, Reference
from datetime import datetime, timedelta
import win32com.client as win32
import os

class CreditLifecycleReporter:
    """Automated reporting for credit lifecycle analytics"""
    
    def __init__(self, connection_string):
        self.conn = pyodbc.connect(connection_string)
        self.report_date = datetime.now()
        
    def extract_originations_data(self):
        """Extract originations performance data"""
        query = """
        SELECT * FROM vw_originations_performance
        WHERE application_date >= DATEADD(month, -3, GETDATE())
        ORDER BY application_date DESC
        """
        return pd.read_sql(query, self.conn)
    
    def extract_collections_data(self):
        """Extract collections performance with root cause analysis"""
        query = "EXEC sp_collections_root_cause_analysis"
        return pd.read_sql(query, self.conn)
    
    def extract_account_management_data(self):
        """Extract account management KPIs"""
        query = "SELECT * FROM vw_account_management_kpis"
        return pd.read_sql(query, self.conn)
    
    def generate_excel_report(self, output_path):
        """Generate comprehensive Excel report with charts"""
        
        # Extract all data
        orig_df = self.extract_originations_data()
        coll_df = self.extract_collections_data()
        acct_df = self.extract_account_management_data()
        
        # Create Excel workbook
        wb = openpyxl.Workbook()
        
        # Originations sheet
        ws_orig = wb.active
        ws_orig.title = "Originations"
        self._write_dataframe_to_sheet(ws_orig, orig_df, "Originations Performance")
        self._add_originations_chart(ws_orig, len(orig_df))
        
        # Collections sheet
        ws_coll = wb.create_sheet("Collections")
        self._write_dataframe_to_sheet(ws_coll, coll_df, "Collections Root Cause Analysis")
        self._add_pareto_chart(ws_coll, len(coll_df))
        
        # Account Management sheet
        ws_acct = wb.create_sheet("Account Management")
        self._write_dataframe_to_sheet(ws_acct, acct_df, "Account Management KPIs")
        
        # Save workbook
        wb.save(output_path)
        print(f"Excel report generated: {output_path}")
        
    def _write_dataframe_to_sheet(self, ws, df, title):
        """Write DataFrame to Excel sheet with formatting"""
        
        # Title
        ws['A1'] = title
        ws['A1'].font = Font(size=14, bold=True)
        ws['A1'].fill = PatternFill(start_color="366092", fill_type="solid")
        ws['A1'].font = Font(size=14, bold=True, color="FFFFFF")
        
        # Headers
        for col_num, column_title in enumerate(df.columns, 1):
            cell = ws.cell(row=3, column=col_num)
            cell.value = column_title
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="D9E1F2", fill_type="solid")
        
        # Data
        for row_num, row_data in enumerate(df.values, 4):
            for col_num, cell_value in enumerate(row_data, 1):
                ws.cell(row=row_num, column=col_num, value=cell_value)
    
    def generate_powerpoint_presentation(self, excel_path, output_path):
        """Generate automated PowerPoint presentation"""
        
        ppt = win32.Dispatch('PowerPoint.Application')
        ppt.Visible = True
        
        # Create presentation
        presentation = ppt.Presentations.Add()
        
        # Title slide
        slide1 = presentation.Slides.Add(1, 1)  # ppLayoutTitle
        slide1.Shapes.Title.TextFrame.TextRange.Text = "Credit Lifecycle Performance Report"
        slide1.Shapes(2).TextFrame.TextRange.Text = f"Report Date: {self.report_date.strftime('%B %d, %Y')}"
        
        # Originations slide
        slide2 = presentation.Slides.Add(2, 11)  # ppLayoutTitleOnly
        slide2.Shapes.Title.TextFrame.TextRange.Text = "Originations Performance"
        
        # Collections slide
        slide3 = presentation.Slides.Add(3, 11)
        slide3.Shapes.Title.TextFrame.TextRange.Text = "Collections Root Cause Analysis"
        
        # Save presentation
        presentation.SaveAs(output_path)
        presentation.Close()
        ppt.Quit()
        
        print(f"PowerPoint presentation generated: {output_path}")
    
    def automate_daily_reports(self):
        """Automated daily report generation"""
        
        report_folder = f"C:/Reports/{self.report_date.strftime('%Y-%m-%d')}"
        os.makedirs(report_folder, exist_ok=True)
        
        # Generate Excel report
        excel_path = f"{report_folder}/Credit_Lifecycle_Report.xlsx"
        self.generate_excel_report(excel_path)
        
        # Generate PowerPoint presentation
        ppt_path = f"{report_folder}/Credit_Lifecycle_Presentation.pptx"
        self.generate_powerpoint_presentation(excel_path, ppt_path)
        
        return excel_path, ppt_path

# Usage
if __name__ == "__main__":
    conn_string = "DRIVER={SQL Server};SERVER=sql-server;DATABASE=CreditDB;Trusted_Connection=yes"
    reporter = CreditLifecycleReporter(conn_string)
    
    # Run automated daily reports
    excel_file, ppt_file = reporter.automate_daily_reports()
    print(f"Reports generated successfully!")`,

      msaccess: `' MS Access VBA - Credit Data Management System
' Automated data import, validation, and export

Option Compare Database
Option Explicit

' Main automation routine
Public Sub AutomateDataRefresh()
    On Error GoTo ErrorHandler
    
    DoCmd.SetWarnings False
    
    ' Step 1: Import data from SQL Server
    Call ImportFromSQLServer
    
    ' Step 2: Validate data quality
    Call ValidateDataQuality
    
    ' Step 3: Run business logic transformations
    Call ApplyBusinessRules
    
    ' Step 4: Export to Excel for distribution
    Call ExportToExcel
    
    DoCmd.SetWarnings True
    MsgBox "Data refresh completed successfully!", vbInformation
    Exit Sub
    
ErrorHandler:
    DoCmd.SetWarnings True
    MsgBox "Error: " & Err.Description, vbCritical
End Sub

' Import data from SQL Server
Private Sub ImportFromSQLServer()
    Dim conn As Object
    Dim rs As Object
    Dim sql As String
    Dim db As DAO.Database
    
    Set conn = CreateObject("ADODB.Connection")
    Set rs = CreateObject("ADODB.Recordset")
    Set db = CurrentDb
    
    ' Connection string
    conn.ConnectionString = "Provider=SQLOLEDB;Data Source=sql-server;" & _
                           "Initial Catalog=CreditDB;Integrated Security=SSPI;"
    conn.Open
    
    ' Import originations data
    sql = "SELECT * FROM credit_originations WHERE application_date >= DATEADD(day, -30, GETDATE())"
    rs.Open sql, conn
    
    ' Clear existing data
    db.Execute "DELETE FROM tbl_Originations"
    
    ' Import records
    Do While Not rs.EOF
        db.Execute "INSERT INTO tbl_Originations (ApplicationID, CustomerName, " & _
                  "LoanAmount, Status, ApplicationDate) VALUES (" & _
                  rs("application_id") & ", '" & rs("customer_name") & "', " & _
                  rs("loan_amount") & ", '" & rs("status") & "', " & _
                  "#" & rs("application_date") & "#)"
        rs.MoveNext
    Loop
    
    rs.Close
    conn.Close
    
    Set rs = Nothing
    Set conn = Nothing
    Set db = Nothing
End Sub

' Validate data quality
Private Sub ValidateDataQuality()
    Dim db As DAO.Database
    Dim rs As DAO.Recordset
    Dim errorCount As Integer
    
    Set db = CurrentDb
    Set rs = db.OpenRecordset("tbl_Originations")
    
    errorCount = 0
    
    Do While Not rs.EOF
        ' Check for missing critical fields
        If IsNull(rs("CustomerName")) Or IsNull(rs("LoanAmount")) Then
            rs.Edit
            rs("ValidationStatus") = "Error: Missing Data"
            rs.Update
            errorCount = errorCount + 1
        
        ' Check for invalid loan amounts
        ElseIf rs("LoanAmount") <= 0 Or rs("LoanAmount") > 10000000 Then
            rs.Edit
            rs("ValidationStatus") = "Error: Invalid Amount"
            rs.Update
            errorCount = errorCount + 1
        
        Else
            rs.Edit
            rs("ValidationStatus") = "Valid"
            rs.Update
        End If
        
        rs.MoveNext
    Loop
    
    rs.Close
    Set rs = Nothing
    Set db = Nothing
    
    If errorCount > 0 Then
        MsgBox errorCount & " validation errors found. Check ValidationStatus field.", vbExclamation
    End If
End Sub

' Apply business rules and calculations
Private Sub ApplyBusinessRules()
    Dim db As DAO.Database
    
    Set db = CurrentDb
    
    ' Calculate risk scores
    db.Execute "UPDATE tbl_Originations SET RiskScore = " & _
              "SWITCH(LoanAmount < 100000, 'Low', " & _
              "LoanAmount >= 100000 AND LoanAmount < 500000, 'Medium', " & _
              "LoanAmount >= 500000, 'High')"
    
    ' Calculate approval probability
    db.Execute "UPDATE tbl_Originations SET ApprovalProbability = " & _
              "SWITCH(Status = 'Approved', 100, " & _
              "Status = 'Pending', 50, " & _
              "Status = 'Rejected', 0)"
    
    Set db = Nothing
End Sub

' Export to Excel
Private Sub ExportToExcel()
    Dim excelApp As Object
    Dim wb As Object
    Dim ws As Object
    Dim db As DAO.Database
    Dim rs As DAO.Recordset
    Dim row As Integer
    
    Set excelApp = CreateObject("Excel.Application")
    Set wb = excelApp.Workbooks.Add
    Set ws = wb.Worksheets(1)
    
    Set db = CurrentDb
    Set rs = db.OpenRecordset("SELECT * FROM tbl_Originations WHERE ValidationStatus = 'Valid'")
    
    ' Headers
    ws.Cells(1, 1).Value = "Application ID"
    ws.Cells(1, 2).Value = "Customer Name"
    ws.Cells(1, 3).Value = "Loan Amount"
    ws.Cells(1, 4).Value = "Status"
    ws.Cells(1, 5).Value = "Risk Score"
    
    ' Format headers
    ws.Range("A1:E1").Font.Bold = True
    ws.Range("A1:E1").Interior.Color = RGB(68, 114, 196)
    ws.Range("A1:E1").Font.Color = RGB(255, 255, 255)
    
    ' Data
    row = 2
    Do While Not rs.EOF
        ws.Cells(row, 1).Value = rs("ApplicationID")
        ws.Cells(row, 2).Value = rs("CustomerName")
        ws.Cells(row, 3).Value = rs("LoanAmount")
        ws.Cells(row, 4).Value = rs("Status")
        ws.Cells(row, 5).Value = rs("RiskScore")
        row = row + 1
        rs.MoveNext
    Loop
    
    ' Auto-fit columns
    ws.Columns("A:E").AutoFit
    
    ' Save file
    wb.SaveAs "C:\\Reports\\Credit_Originations_" & Format(Date, "yyyy-mm-dd") & ".xlsx"
    wb.Close
    excelApp.Quit
    
    rs.Close
    Set rs = Nothing
    Set db = Nothing
    Set ws = Nothing
    Set wb = Nothing
    Set excelApp = Nothing
End Sub`,

      powerpoint: `' PowerPoint VBA - Automated Presentation Generation
' Creates executive presentations with charts and data

Sub GenerateCreditLifecyclePresentation()
    On Error GoTo ErrorHandler
    
    Dim pptApp As PowerPoint.Application
    Dim pptPres As PowerPoint.Presentation
    Dim pptSlide As PowerPoint.Slide
    Dim pptShape As PowerPoint.Shape
    Dim pptChart As PowerPoint.Chart
    
    ' Create PowerPoint application
    Set pptApp = New PowerPoint.Application
    pptApp.Visible = True
    
    ' Create new presentation
    Set pptPres = pptApp.Presentations.Add
    
    ' Slide 1: Title Slide
    Set pptSlide = pptPres.Slides.Add(1, ppLayoutTitle)
    pptSlide.Shapes.Title.TextFrame.TextRange.Text = "Credit Lifecycle Performance Report"
    pptSlide.Shapes(2).TextFrame.TextRange.Text = "Business & Commercial Banking" & vbCrLf & _
                                                   "Report Date: " & Format(Date, "mmmm dd, yyyy")
    
    ' Format title slide
    With pptSlide.Shapes.Title.TextFrame.TextRange.Font
        .Name = "Calibri"
        .Size = 44
        .Bold = True
        .Color.RGB = RGB(68, 114, 196)
    End With
    
    ' Slide 2: Originations Performance
    Set pptSlide = pptPres.Slides.Add(2, ppLayoutTitleOnly)
    pptSlide.Shapes.Title.TextFrame.TextRange.Text = "Originations Performance - Q4 2024"
    
    ' Add chart
    Set pptShape = pptSlide.Shapes.AddChart2(227, xlColumnClustered, 50, 100, 600, 400)
    Set pptChart = pptShape.Chart
    
    ' Populate chart data
    With pptChart.ChartData.Workbook.Worksheets(1)
        .Cells(1, 1).Value = "Month"
        .Cells(1, 2).Value = "Applications"
        .Cells(1, 3).Value = "Approvals"
        
        .Cells(2, 1).Value = "October"
        .Cells(2, 2).Value = 450
        .Cells(2, 3).Value = 380
        
        .Cells(3, 1).Value = "November"
        .Cells(3, 2).Value = 520
        .Cells(3, 3).Value = 445
        
        .Cells(4, 1).Value = "December"
        .Cells(4, 2).Value = 490
        .Cells(4, 3).Value = 425
    End With
    
    ' Format chart
    With pptChart
        .HasTitle = True
        .ChartTitle.Text = "Monthly Originations Trend"
        .ChartTitle.Font.Size = 18
        .ChartTitle.Font.Bold = True
    End With
    
    ' Slide 3: Collections Root Cause Analysis
    Set pptSlide = pptPres.Slides.Add(3, ppLayoutTitleOnly)
    pptSlide.Shapes.Title.TextFrame.TextRange.Text = "Collections - Root Cause Analysis (Pareto)"
    
    ' Add Pareto chart
    Set pptShape = pptSlide.Shapes.AddChart2(227, xlColumnClustered, 50, 100, 600, 400)
    Set pptChart = pptShape.Chart
    
    ' Populate Pareto data
    With pptChart.ChartData.Workbook.Worksheets(1)
        .Cells(1, 1).Value = "Cause"
        .Cells(1, 2).Value = "Cases"
        .Cells(1, 3).Value = "Cumulative %"
        
        .Cells(2, 1).Value = "Job Loss"
        .Cells(2, 2).Value = 145
        .Cells(2, 3).Value = 42
        
        .Cells(3, 1).Value = "Medical Emergency"
        .Cells(3, 2).Value = 98
        .Cells(3, 3).Value = 70
        
        .Cells(4, 1).Value = "Business Failure"
        .Cells(4, 2).Value = 67
        .Cells(4, 3).Value = 90
        
        .Cells(5, 1).Value = "Other"
        .Cells(5, 2).Value = 35
        .Cells(5, 3).Value = 100
    End With
    
    ' Add text box with insights
    Set pptShape = pptSlide.Shapes.AddTextbox(msoTextOrientationHorizontal, 50, 520, 600, 60)
    With pptShape.TextFrame.TextRange
        .Text = "Key Insight: Top 3 causes account for 90% of delinquency cases. " & _
               "Targeted intervention programs recommended for job loss and medical emergency segments."
        .Font.Size = 14
        .Font.Name = "Calibri"
        .Font.Color.RGB = RGB(68, 114, 196)
    End With
    
    ' Slide 4: Account Management KPIs
    Set pptSlide = pptPres.Slides.Add(4, ppLayoutTitleOnly)
    pptSlide.Shapes.Title.TextFrame.TextRange.Text = "Account Management - Portfolio Health"
    
    ' Add KPI table
    Set pptShape = pptSlide.Shapes.AddTable(5, 3, 100, 120, 550, 300)
    
    With pptShape.Table
        ' Headers
        .Cell(1, 1).Shape.TextFrame.TextRange.Text = "Metric"
        .Cell(1, 2).Shape.TextFrame.TextRange.Text = "Current"
        .Cell(1, 3).Shape.TextFrame.TextRange.Text = "Target"
        
        ' Data
        .Cell(2, 1).Shape.TextFrame.TextRange.Text = "Active Accounts"
        .Cell(2, 2).Shape.TextFrame.TextRange.Text = "12,450"
        .Cell(2, 3).Shape.TextFrame.TextRange.Text = "12,000"
        
        .Cell(3, 1).Shape.TextFrame.TextRange.Text = "Portfolio Value (R millions)"
        .Cell(3, 2).Shape.TextFrame.TextRange.Text = "R 2,340"
        .Cell(3, 3).Shape.TextFrame.TextRange.Text = "R 2,200"
        
        .Cell(4, 1).Shape.TextFrame.TextRange.Text = "Avg Credit Score"
        .Cell(4, 2).Shape.TextFrame.TextRange.Text = "685"
        .Cell(4, 3).Shape.TextFrame.TextRange.Text = "680"
        
        .Cell(5, 1).Shape.TextFrame.TextRange.Text = "High Risk Accounts %"
        .Cell(5, 2).Shape.TextFrame.TextRange.Text = "8.5%"
        .Cell(5, 3).Shape.TextFrame.TextRange.Text = "<10%"
    End With
    
    ' Format table
    For i = 1 To 5
        For j = 1 To 3
            With pptShape.Table.Cell(i, j).Shape.TextFrame.TextRange.Font
                .Size = 14
                .Name = "Calibri"
            End With
        Next j
    Next i
    
    ' Slide 5: Recommendations
    Set pptSlide = pptPres.Slides.Add(5, ppLayoutText)
    pptSlide.Shapes.Title.TextFrame.TextRange.Text = "Strategic Recommendations"
    
    With pptSlide.Shapes(2).TextFrame.TextRange
        .Text = "1. Implement automated early warning system for collections" & vbCrLf & vbCrLf & _
               "2. Develop targeted intervention programs for top 3 delinquency causes" & vbCrLf & vbCrLf & _
               "3. Enhance originations approval process to maintain 85%+ approval rate" & vbCrLf & vbCrLf & _
               "4. Expand portfolio management capacity to handle growth" & vbCrLf & vbCrLf & _
               "5. Continue automation initiatives to reduce reporting time"
        .Font.Size = 18
        .Font.Name = "Calibri"
    End With
    
    ' Save presentation
    pptPres.SaveAs "C:\\Reports\\Credit_Lifecycle_Presentation_" & Format(Date, "yyyy-mm-dd") & ".pptx"
    
    MsgBox "Presentation generated successfully!", vbInformation
    Exit Sub
    
ErrorHandler:
    MsgBox "Error generating presentation: " & Err.Description, vbCritical
End Sub`,
    },
  },
}

export default function Portfolio() {
  const [selectedProject, setSelectedProject] = useState<string | null>(null)
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [isResumeOpen, setIsResumeOpen] = useState(false)

  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId)
    if (element) {
      element.scrollIntoView({ behavior: "smooth" })
      setMobileMenuOpen(false) // Close mobile menu after navigation
    }
  }

  return (
    <div className="min-h-screen bg-background text-foreground">
      <nav className="sticky top-0 z-50 bg-background/80 backdrop-blur-md border-b">
        <div className="max-w-6xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="h-10 w-10 rounded-full bg-primary flex items-center justify-center text-primary-foreground font-bold">
                SE
              </div>
              <span className="font-serif font-bold text-xl hidden sm:inline">Stanton Edwards</span>
            </div>

            {/* Desktop Navigation */}
            <div className="hidden md:flex items-center gap-6">
              <button
                onClick={() => scrollToSection("home")}
                className="flex items-center gap-2 text-sm hover:text-primary transition-colors"
              >
                <Home className="h-4 w-4" />
                Home
              </button>
              <button
                onClick={() => scrollToSection("skills")}
                className="flex items-center gap-2 text-sm hover:text-primary transition-colors"
              >
                <Award className="h-4 w-4" />
                Skills
              </button>
              <button
                onClick={() => scrollToSection("experience")}
                className="flex items-center gap-2 text-sm hover:text-primary transition-colors"
              >
                <Briefcase className="h-4 w-4" />
                Experience
              </button>
              <button
                onClick={() => scrollToSection("projects")}
                className="flex items-center gap-2 text-sm hover:text-primary transition-colors"
              >
                <FolderOpen className="h-4 w-4" />
                Projects
              </button>
              <button
                onClick={() => scrollToSection("contact")}
                className="flex items-center gap-2 text-sm hover:text-primary transition-colors"
              >
                <MessageSquare className="h-4 w-4" />
                Contact
              </button>
            </div>

            {/* Mobile Menu Button */}
            <button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="md:hidden p-2 hover:bg-muted rounded-lg transition-colors"
            >
              {mobileMenuOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
            </button>
          </div>

          {/* Mobile Navigation */}
          {mobileMenuOpen && (
            <div className="md:hidden mt-4 pb-4 space-y-2">
              <button
                onClick={() => scrollToSection("home")}
                className="flex items-center gap-3 w-full px-4 py-3 hover:bg-muted rounded-lg transition-colors"
              >
                <Home className="h-5 w-5" />
                <span>Home</span>
              </button>
              <button
                onClick={() => scrollToSection("skills")}
                className="flex items-center gap-3 w-full px-4 py-3 hover:bg-muted rounded-lg transition-colors"
              >
                <Award className="h-5 w-5" />
                <span>Skills</span>
              </button>
              <button
                onClick={() => scrollToSection("experience")}
                className="flex items-center gap-3 w-full px-4 py-3 hover:bg-muted rounded-lg transition-colors"
              >
                <Briefcase className="h-5 w-5" />
                <span>Experience</span>
              </button>
              <button
                onClick={() => scrollToSection("projects")}
                className="flex items-center gap-3 w-full px-4 py-3 hover:bg-muted rounded-lg transition-colors"
              >
                <FolderOpen className="h-5 w-5" />
                <span>Projects</span>
              </button>
              <button
                onClick={() => scrollToSection("contact")}
                className="flex items-center gap-3 w-full px-4 py-3 hover:bg-muted rounded-lg transition-colors"
              >
                <MessageSquare className="h-5 w-5" />
                <span>Contact</span>
              </button>
            </div>
          )}
        </div>
      </nav>

      {/* Hero Section */}
      <section
        id="home"
        className="min-h-screen flex items-center justify-center px-4 relative py-20 px-4 text-center bg-gradient-to-br from-muted to-card pt-32"
      >
        <div className="max-w-4xl mx-auto">
          <div className="mb-8">
            <img
              src="/images/stanton-edwards-professional.jpg"
              alt="Stanton Edwards - Professional headshot"
              className="w-32 h-32 md:w-40 md:h-40 rounded-full mx-auto object-cover object-[center_10%] border-4 border-primary/20 shadow-lg"
            />
          </div>
          <h1 className="text-5xl md:text-7xl font-serif font-bold mb-6 text-balance">Stanton Edwards</h1>
          <p className="text-xl md:text-2xl text-muted-foreground mb-8 text-pretty">
            Senior Data Engineer | Big Data Enthusiast | Analytics Expert
          </p>
          <p className="text-lg text-card-foreground max-w-2xl mx-auto mb-8 leading-relaxed text-pretty">
            Transforming raw data into actionable business insights through scalable infrastructure, advanced analytics,
            and cutting-edge big data technologies.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button size="lg" className="bg-primary hover:bg-primary/90" asChild>
              <a href="#contact">
                <Mail className="mr-2 h-4 w-4" />
                Get In Touch
              </a>
            </Button>
            <Button variant="outline" size="lg" className="bg-transparent" onClick={() => setIsResumeOpen(true)}>
              <FileText className="h-5 w-5" />
              View CV
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
                  <Brain className="h-8 w-8 text-primary" />
                  <CardTitle>AI & Machine Learning</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Machine Learning & Deep Learning</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Natural Language Processing</span>
                      <span className="text-sm text-muted-foreground">90%</span>
                    </div>
                    <Progress value={90} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Predictive Analytics</span>
                      <span className="text-sm text-muted-foreground">93%</span>
                    </div>
                    <Progress value={93} className="h-2" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <TrendingUp className="h-8 w-8 text-primary" />
                  <CardTitle>Analytics & Visualization</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Tableau & Looker</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Advanced Excel & Power BI</span>
                      <span className="text-sm text-muted-foreground">92%</span>
                    </div>
                    <Progress value={92} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Statistical Analysis (SAS/R)</span>
                      <span className="text-sm text-muted-foreground">90%</span>
                    </div>
                    <Progress value={90} className="h-2" />
                  </div>
                </div>
              </CardContent>
            </Card>

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
                      <span>Real-time Data Processing</span>
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
                  <CardTitle>Cloud Computing</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>AWS (EC2, EMR, Redshift, SageMaker)</span>
                      <span className="text-sm text-muted-foreground">92%</span>
                    </div>
                    <Progress value={92} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Azure ML & Data Services</span>
                      <span className="text-sm text-muted-foreground">88%</span>
                    </div>
                    <Progress value={88} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>GCP BigQuery & AI Platform</span>
                      <span className="text-sm text-muted-foreground">85%</span>
                    </div>
                    <Progress value={85} className="h-2" />
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
                      <span>Python (Pandas, NumPy, Scikit-learn)</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>R (Statistical Modeling)</span>
                      <span className="text-sm text-muted-foreground">90%</span>
                    </div>
                    <Progress value={90} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>SQL & SAS</span>
                      <span className="text-sm text-muted-foreground">93%</span>
                    </div>
                    <Progress value={93} className="h-2" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <Users className="h-8 w-8 text-primary" />
                  <CardTitle>Leadership & Strategy</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Team Leadership & Mentoring</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Analytics Strategy Development</span>
                      <span className="text-sm text-muted-foreground">92%</span>
                    </div>
                    <Progress value={92} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Stakeholder Management</span>
                      <span className="text-sm text-muted-foreground">90%</span>
                    </div>
                    <Progress value={90} className="h-2" />
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
            <Card className="border-2 border-primary">
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">Data Analytics & AI Lead</CardTitle>
                    <CardDescription className="text-base">
                      TotalEnergies • Finance & IS Management • Rosebank Johannesburg
                    </CardDescription>
                  </div>
                  <Badge className="w-fit bg-primary">Current Role</Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>
                    • Lead a team of 25+ data scientists, analysts, and ML engineers across advanced analytics, AI/ML,
                    and business intelligence functions
                  </li>
                  <li>
                    • Developed and executed enterprise analytics strategy aligned with Standard Bank Group's digital
                    transformation, delivering R150M+ in measurable business value
                  </li>
                  <li>
                    • Built best-in-class customer insights and personalization platform using ML/NLP, increasing
                    customer retention by 28% and cross-sell conversion by 35%
                  </li>
                  <li>
                    • Implemented real-time risk analytics and automated decision-making systems processing 5M+
                    transactions daily with 99.2% accuracy
                  </li>
                  <li>
                    • Established AI ethics framework and governance policies ensuring fairness, transparency, and POPIA
                    compliance across all ML models
                  </li>
                  <li>
                    • Championed data-driven culture through executive dashboards (Tableau/Looker) and self-service
                    analytics, enabling 500+ business users
                  </li>
                  <li>
                    • Led predictive analytics initiatives for insurance underwriting and claims optimization, reducing
                    loss ratios by 18% and improving operational efficiency by 40%
                  </li>
                </ul>
              </CardContent>
            </Card>

            {/* Updated Experience Entry */}
            <Card>
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">BI Solutions Architect - Data & Analytics</CardTitle>
                    <CardDescription className="text-base">
                      Retail and B2B • TotalEnergies • 2022 - 2024
                    </CardDescription>
                  </div>
                  <Badge variant="outline" className="w-fit">
                    2 Years
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>
                    • Designed scalable, secure, and high-performance data solutions aligned with business requirements
                    and digital transformation initiatives
                  </li>
                  <li>
                    • Architected cloud-native data solutions across Azure, AWS, and GCP, implementing data lakes,
                    warehouses, and lakehouses
                  </li>
                  <li>
                    • Developed conceptual, logical, and physical data models ensuring data consistency, quality, and
                    lineage across enterprise systems
                  </li>
                  <li>
                    • Embedded data governance principles and ensured compliance with POPIA and GDPR regulations through
                    robust security controls
                  </li>
                  <li>
                    • Led technical architecture reviews and mentored data engineering teams on best practices using
                    TOGAF frameworks
                  </li>
                </ul>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">Senior Data Engineer & Analytics Lead</CardTitle>
                    <CardDescription className="text-base">Retail and B2B • TotalEnergies</CardDescription>
                  </div>
                  <Badge variant="outline" className="w-fit">
                    3 Years
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>
                    • Led analytics team of 8 engineers delivering advanced analytics solutions and ML model deployment
                  </li>
                  <li>
                    • Built customer segmentation and propensity models using Python/R, driving 45% improvement in
                    marketing campaign ROI
                  </li>
                  <li>• Optimized Spark jobs reducing processing time by 70% and infrastructure costs by 45%</li>
                  <li>
                    • Implemented real-time streaming analytics processing 50M+ events daily with sub-second latency
                  </li>
                  <li>• Established data quality frameworks and automated testing, reducing data incidents by 85%</li>
                </ul>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                  <div>
                    <CardTitle className="text-xl">Data Engineer & Business Analyst</CardTitle>
                    <CardDescription className="text-base">DataFlow Analytics • 2016 - 2019</CardDescription>
                  </div>
                  <Badge variant="outline" className="w-fit">
                    3 Years
                  </Badge>
                </div>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm text-muted-foreground">
                  <li>• Built and maintained ETL pipelines processing 100GB+ daily using Apache Airflow and Python</li>
                  <li>
                    • Developed statistical models in Python/R for customer behavior analysis and churn prediction
                  </li>
                  <li>
                    • Created executive dashboards in Tableau combining complex data signals into actionable insights
                  </li>
                  <li>• Collaborated with data scientist to productionize ML models serving 1M+ predictions daily</li>
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

            {/* HR Data Platform Project */}
            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Enterprise HR Data Platform</CardTitle>
                    <CardDescription className="text-base">
                      Developed a unified platform for workforce analytics, compliance, and reporting
                    </CardDescription>
                  </div>
                  <Database className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Built a Medallion architecture data platform on Azure Data Lake and SQL DW, integrating data from 8+
                    HRIS systems. Enabled advanced workforce analytics, ensured compliance, and automated executive
                    reporting, reducing delays from 2 weeks to 1 day.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">Azure Data Lake</Badge>
                    <Badge variant="secondary">Azure SQL DW</Badge>
                    <Badge variant="secondary">Python</Badge>
                    <Badge variant="secondary">Alteryx</Badge>
                    <Badge variant="secondary">Power BI</Badge>
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
                            {projectDetails["hr-data-platform"].title}
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
                                  {projectDetails["hr-data-platform"].problemStatement}
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
                                  src={projectDetails["hr-data-platform"].architecture || "/placeholder.svg"}
                                  alt="HR Data Platform Architecture"
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
                                      <li>• Medallion architecture (Bronze, Silver, Gold)</li>
                                      <li>• Azure Data Lake Storage Gen2</li>
                                      <li>• Azure Synapse Analytics / SQL DW</li>
                                      <li>• Python for ETL orchestration & scripting</li>
                                      <li>• Alteryx for complex transformations</li>
                                      <li>• Power BI for executive dashboards</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Reduced reporting delays from 2 weeks to 1 day</li>
                                      <li>• Unified view of 100% of workforce data</li>
                                      <li>• Improved compliance with labor regulations</li>
                                      <li>• 30% reduction in manual data reconciliation effort</li>
                                      <li>• Enabled advanced workforce analytics capabilities</li>
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
                                <TabsTrigger value="alteryx">Alteryx</TabsTrigger>
                                <TabsTrigger value="powerbi">Power BI</TabsTrigger>
                              </TabsList>
                              <TabsContent value="sql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      SQL - Medallion Architecture Schema
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["hr-data-platform"].solution.sql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - ETL Orchestration & Automation
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["hr-data-platform"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="alteryx">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Server className="h-5 w-5" />
                                      Alteryx - Data Integration Workflow
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["hr-data-platform"].solution.alteryx}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="powerbi">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <BarChart3 className="h-5 w-5" />
                                      Power BI - DAX Measures for Analytics
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["hr-data-platform"].solution.powerbi}</code>
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

            {/* Insurance Analytics Project */}
            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Insurance Analytics & Insights Platform</CardTitle>
                    <CardDescription className="text-base">
                      Built comprehensive analytics platform for insurance insights, claims analysis, and customer
                      segmentation
                    </CardDescription>
                  </div>
                  <TrendingUp className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Developed an end-to-end analytics platform processing data from multiple insurance systems, enabling
                    real-time insights on policy performance, claims trends, and customer behavior. Reduced reporting
                    time from 7 days to 1 day and enabled data-driven decision making across the organization.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">SQL Server</Badge>
                    <Badge variant="secondary">Python</Badge>
                    <Badge variant="secondary">Power BI</Badge>
                    <Badge variant="secondary">Azure</Badge>
                    <Badge variant="secondary">Machine Learning</Badge>
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
                            {projectDetails["insurance-analytics"].title}
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
                                  {projectDetails["insurance-analytics"].problemStatement}
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
                                  src={projectDetails["insurance-analytics"].architecture || "/placeholder.svg"}
                                  alt="Insurance Analytics Architecture"
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
                                      <li>• SQL Server for relational data modeling</li>
                                      <li>• Python for data processing & ML models</li>
                                      <li>• Power BI for interactive dashboards</li>
                                      <li>• Azure cloud infrastructure</li>
                                      <li>• Root cause analysis algorithms</li>
                                      <li>• Customer segmentation & churn prediction</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Reduced reporting time from 7 days to 1 day</li>
                                      <li>• 85% accuracy in churn prediction models</li>
                                      <li>• Identified top 20% of claims causes (Pareto)</li>
                                      <li>• Enabled proactive risk management</li>
                                      <li>• Improved customer segmentation insights</li>
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
                                <TabsTrigger value="powerbi">Power BI</TabsTrigger>
                              </TabsList>
                              <TabsContent value="sql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      SQL - Insurance Analytics Data Models
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["insurance-analytics"].solution.sql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - Analytics & Machine Learning
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["insurance-analytics"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="powerbi">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <BarChart3 className="h-5 w-5" />
                                      Power BI - DAX Measures & KPIs
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["insurance-analytics"].solution.powerbi}</code>
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

            {/* Credit Lifecycle BI Project */}
            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Credit Lifecycle BI & Reporting Platform</CardTitle>
                    <CardDescription className="text-base">
                      Automated BI platform for credit lifecycle insights across originations, account management, and
                      collections
                    </CardDescription>
                  </div>
                  <FileSpreadsheet className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Built a comprehensive BI platform for Business & Commercial Banking, automating credit lifecycle
                    reporting and reducing report generation time from 7 days to 1 day. Integrated data from multiple
                    systems using SQL, Python, MS Access, and PowerPoint for executive presentations.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">SQL Server</Badge>
                    <Badge variant="secondary">Python</Badge>
                    <Badge variant="secondary">MS Access</Badge>
                    <Badge variant="secondary">PowerPoint VBA</Badge>
                    <Badge variant="secondary">Excel Automation</Badge>
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
                            {projectDetails["credit-lifecycle-bi"].title}
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
                                  {projectDetails["credit-lifecycle-bi"].problemStatement}
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
                                  src={projectDetails["credit-lifecycle-bi"].architecture || "/placeholder.svg"}
                                  alt="Credit Lifecycle BI Architecture"
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
                                      <li>• SQL Server for data warehousing</li>
                                      <li>• Python for automation & report generation</li>
                                      <li>• MS Access for data management & validation</li>
                                      <li>• PowerPoint VBA for executive presentations</li>
                                      <li>• Excel automation with openpyxl</li>
                                      <li>• Root cause analysis & Pareto charts</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Reduced reporting time from 7 days to 1 day</li>
                                      <li>• 100% automation of daily production reports</li>
                                      <li>• Improved data quality and consistency</li>
                                      <li>• Enabled real-time portfolio visibility</li>
                                      <li>• Standardized reporting across teams</li>
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
                                <TabsTrigger value="msaccess">MS Access</TabsTrigger>
                                <TabsTrigger value="powerpoint">PowerPoint</TabsTrigger>
                              </TabsList>
                              <TabsContent value="sql">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Database className="h-5 w-5" />
                                      SQL - Credit Lifecycle Analytics
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["credit-lifecycle-bi"].solution.sql}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="python">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Code className="h-5 w-5" />
                                      Python - Automated Report Generation
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["credit-lifecycle-bi"].solution.python}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="msaccess">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <Server className="h-5 w-5" />
                                      MS Access VBA - Data Management
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["credit-lifecycle-bi"].solution.msaccess}</code>
                                    </pre>
                                  </CardContent>
                                </Card>
                              </TabsContent>
                              <TabsContent value="powerpoint">
                                <Card>
                                  <CardHeader>
                                    <CardTitle className="flex items-center gap-2">
                                      <FileSpreadsheet className="h-5 w-5" />
                                      PowerPoint VBA - Executive Presentations
                                    </CardTitle>
                                  </CardHeader>
                                  <CardContent>
                                    <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                      <code>{projectDetails["credit-lifecycle-bi"].solution.powerpoint}</code>
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
      <section id="contact" className="py-16 px-4">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-4xl font-serif font-bold text-center mb-12 text-balance">Get In Touch</h2>
          <div className="grid md:grid-cols-3 gap-6">
            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <Mail className="h-6 w-6 text-primary" />
                  <CardTitle className="text-lg">Email</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <a
                  href="mailto:stanton.edwards@outlook.com"
                  className="text-sm text-muted-foreground hover:text-primary transition-colors"
                >
                  stanton.edwards@outlook.com
                </a>
              </CardContent>
            </Card>

            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <Phone className="h-6 w-6 text-primary" />
                  <CardTitle className="text-lg">Phone</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <a href="tel:0798810997" className="text-sm text-muted-foreground hover:text-primary transition-colors">
                  079 881 0997
                </a>
              </CardContent>
            </Card>

            <Card className="hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <MapPin className="h-6 w-6 text-primary" />
                  <CardTitle className="text-lg">Location</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <p className="text-sm text-muted-foreground">Johannesburg, South Africa</p>
              </CardContent>
            </Card>
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

      <Dialog open={isResumeOpen} onOpenChange={setIsResumeOpen}>
        <DialogContent className="max-w-5xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="text-3xl font-serif">CV - Stanton Edwards</DialogTitle>
          </DialogHeader>
          <div className="space-y-6 print:space-y-4">
            {/* Contact Information */}
            <div className="border-b pb-4">
              <h2 className="text-2xl font-serif font-bold mb-2">Contact Information</h2>
              <div className="grid md:grid-cols-3 gap-4 text-sm">
                <div className="flex items-center gap-2">
                  <Mail className="h-4 w-4 text-primary" />
                  <span>stanton.edwards@outlook.com</span>
                </div>
                <div className="flex items-center gap-2">
                  <Phone className="h-4 w-4 text-primary" />
                  <span>079 881 0997</span>
                </div>
                <div className="flex items-center gap-2">
                  <MapPin className="h-4 w-4 text-primary" />
                  <span>Johannesburg, South Africa</span>
                </div>
              </div>
            </div>

            {/* Professional Summary */}
            <div className="border-b pb-4">
              <h2 className="text-2xl font-serif font-bold mb-3">Professional Summary</h2>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Accomplished data analytics and AI leader with 10+ years of experience driving digital transformation
                through advanced analytics, machine learning, and big data technologies. Proven track record of building
                and leading high-performing teams, delivering R150M+ in measurable business value, and implementing
                enterprise-scale data solutions. Expert in customer insights, predictive analytics, real-time
                processing, and AI ethics. Strong background in cloud computing (AWS, Azure, GCP), statistical analysis
                (Python, R, SAS), and data visualization (Tableau, Looker). Passionate about leveraging data as a
                strategic asset to drive business outcomes and innovation.
              </p>
            </div>

            {/* Professional Experience */}
            <div className="border-b pb-4">
              <h2 className="text-2xl font-serif font-bold mb-3">Professional Experience</h2>
              <div className="space-y-4">
                {/* Current Role */}
                <div>
                  <div className="flex justify-between items-start mb-2">
                    <div>
                      <h3 className="font-bold text-lg">Head of Data Analytics & AI</h3>
                      <p className="text-sm text-muted-foreground">
                        Standard Bank - Insurance & Asset Management, Johannesburg
                      </p>
                    </div>
                    <Badge className="bg-primary">Current</Badge>
                  </div>
                  <ul className="text-sm text-muted-foreground space-y-1 ml-4 list-disc">
                    <li>
                      Lead team of 25+ data scientists, analysts, and ML engineers across advanced analytics and AI/ML
                      functions
                    </li>
                    <li>
                      Developed enterprise analytics strategy delivering R150M+ in measurable business value through
                      customer insights and risk analytics
                    </li>
                    <li>
                      Built customer personalization platform using ML/NLP, increasing retention by 28% and cross-sell
                      conversion by 35%
                    </li>
                    <li>Implemented real-time risk analytics processing 5M+ transactions daily with 99.2% accuracy</li>
                    <li>
                      Established AI ethics framework ensuring fairness, transparency, and POPIA compliance across all
                      ML models
                    </li>
                    <li>
                      Led predictive analytics for insurance underwriting, reducing loss ratios by 18% and improving
                      efficiency by 40%
                    </li>
                  </ul>
                </div>

                {/* Previous Role */}
                <div>
                  <div className="flex justify-between items-start mb-2">
                    <div>
                      <h3 className="font-bold text-lg">BI Solutions Architect - Data & Analytics</h3>
                      <p className="text-sm text-muted-foreground">
                        TotalEnergies • Finance & IS • Rosebank Johannesburg
                      </p>
                    </div>
                    <span className="text-sm text-muted-foreground">2022 - 2024</span>
                  </div>
                  <ul className="text-sm text-muted-foreground space-y-1 ml-4 list-disc">
                    <li>
                      Designed scalable, secure, and high-performance data solutions aligned with business requirements
                      and digital transformation initiatives
                    </li>
                    <li>
                      Architected cloud-native data solutions across Azure, AWS, and GCP, implementing data lakes,
                      warehouses, and lakehouses
                    </li>
                    <li>
                      Developed conceptual, logical, and physical data models ensuring data consistency, quality, and
                      lineage across enterprise systems
                    </li>
                    <li>
                      Embedded data governance principles and ensured compliance with POPIA and GDPR regulations through
                      robust security controls
                    </li>
                    <li>
                      Led technical architecture reviews and mentored data engineering teams on best practices using
                      TOGAF frameworks
                    </li>
                  </ul>
                </div>

                {/* Earlier Roles */}
                <div>
                  <div className="flex justify-between items-start mb-2">
                    <div>
                      <h3 className="font-bold text-lg">Senior Data Engineer & Analytics Lead</h3>
                      <p className="text-sm text-muted-foreground">Retail and B2B • TotalEnergies</p>
                    </div>
                    <span className="text-sm text-muted-foreground">2019 - 2022</span>
                  </div>
                  <ul className="text-sm text-muted-foreground space-y-1 ml-4 list-disc">
                    <li>Led analytics team of 8 engineers delivering advanced analytics and ML model deployment</li>
                    <li>Built customer segmentation models using Python/R, driving 45% improvement in marketing ROI</li>
                    <li>Optimized Spark jobs reducing processing time by 70% and infrastructure costs by 45%</li>
                  </ul>
                </div>

                <div>
                  <div className="flex justify-between items-start mb-2">
                    <div>
                      <h3 className="font-bold text-lg">Data Engineer & Business Analyst</h3>
                      <p className="text-sm text-muted-foreground">DataFlow Analytics</p>
                    </div>
                    <span className="text-sm text-muted-foreground">2016 - 2019</span>
                  </div>
                  <ul className="text-sm text-muted-foreground space-y-1 ml-4 list-disc">
                    <li>Built ETL pipelines processing 100GB+ daily using Apache Airflow and Python</li>
                    <li>
                      Developed statistical models in Python/R for customer behavior analysis and churn prediction
                    </li>
                    <li>Created executive dashboards in Tableau combining data signals into actionable insights</li>
                    <li>Collaborated with data scientist to productionize ML models serving 1M+ predictions daily</li>
                  </ul>
                </div>
              </div>
            </div>

            {/* Technical Skills */}
            <div className="border-b pb-4">
              <h2 className="text-2xl font-serif font-bold mb-3">Technical Skills</h2>
              <div className="grid md:grid-cols-2 gap-4 text-sm">
                <div>
                  <h3 className="font-semibold mb-2">AI & Machine Learning</h3>
                  <p className="text-muted-foreground">
                    Machine Learning, Deep Learning, Natural Language Processing, Predictive Analytics, AI Ethics
                  </p>
                </div>
                <div>
                  <h3 className="font-semibold mb-2">Analytics & Visualization</h3>
                  <p className="text-muted-foreground">
                    Tableau, Looker, Power BI, Advanced Excel, Statistical Analysis (SAS/R)
                  </p>
                </div>
                <div>
                  <h3 className="font-semibold mb-2">Big Data Technologies</h3>
                  <p className="text-muted-foreground">
                    Apache Spark, Hadoop, Kafka, Real-time Processing, Automated Decision-Making
                  </p>
                </div>
                <div>
                  <h3 className="font-semibold mb-2">Cloud Computing</h3>
                  <p className="text-muted-foreground">
                    AWS (EC2, EMR, Redshift, SageMaker), Azure ML, GCP BigQuery, Cloud Architecture
                  </p>
                </div>
                <div>
                  <h3 className="font-semibold mb-2">Programming Languages</h3>
                  <p className="text-muted-foreground">Python (Pandas, NumPy, Scikit-learn), R, SQL, SAS, Scala</p>
                </div>
                <div>
                  <h3 className="font-semibold mb-2">Leadership & Strategy</h3>
                  <p className="text-muted-foreground">
                    Team Leadership, Analytics Strategy, Stakeholder Management, TOGAF Frameworks
                  </p>
                </div>
              </div>
            </div>

            {/* Education */}
            <div className="border-b pb-4">
              <h2 className="text-2xl font-serif font-bold mb-3">Education</h2>
              <div className="space-y-3">
                <div>
                  <h3 className="font-bold">Master of Science in Data Science</h3>
                  <p className="text-sm text-muted-foreground">University of Johannesburg</p>
                  <p className="text-sm text-muted-foreground">
                    Specialization: Machine Learning & Statistical Analysis
                  </p>
                </div>
                <div>
                  <h3 className="font-bold">Bachelor of Science in Computer Science</h3>
                  <p className="text-sm text-muted-foreground">University of Cape Town</p>
                  <p className="text-sm text-muted-foreground">Focus: Data Structures & Algorithms</p>
                </div>
              </div>
            </div>

            {/* Certifications */}
            <div className="border-b pb-4">
              <h2 className="text-2xl font-serif font-bold mb-3">Certifications</h2>
              <div className="grid md:grid-cols-2 gap-2 text-sm">
                <div className="flex items-center gap-2">
                  <Badge variant="outline">AWS Certified Solutions Architect</Badge>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">TOGAF 9 Certified</Badge>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">Azure Data Engineer Associate</Badge>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">Google Cloud Professional Data Engineer</Badge>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">Tableau Desktop Specialist</Badge>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">Apache Spark Developer</Badge>
                </div>
              </div>
            </div>

            {/* Key Projects */}
            <div>
              <h2 className="text-2xl font-serif font-bold mb-3">Key Projects</h2>
              <div className="space-y-3 text-sm">
                <div>
                  <h3 className="font-bold">Real-time Fraud Detection Pipeline</h3>
                  <p className="text-muted-foreground">
                    Built scalable fraud detection system processing 1M+ transactions daily with 98.5% accuracy using
                    Kafka, Spark Streaming, and ML models
                  </p>
                </div>
                <div>
                  <h3 className="font-bold">Customer Analytics Data Warehouse</h3>
                  <p className="text-muted-foreground">
                    Architected multi-terabyte data warehouse on AWS Redshift with automated ETL, increasing marketing
                    ROI by 35%
                  </p>
                </div>
                <div>
                  <h3 className="font-bold">IoT Sensor Data Processing Platform</h3>
                  <p className="text-muted-foreground">
                    Developed distributed system handling 10M+ sensor readings per hour with predictive maintenance
                    algorithms
                  </p>
                </div>
                <div>
                  <h3 className="font-bold">Financial Risk Analytics Engine</h3>
                  <p className="text-muted-foreground">
                    Built risk analytics platform using advanced statistical models, improving accuracy by 30% and
                    reducing processing time by 60%
                  </p>
                </div>
                <div>
                  <h3 className="font-bold">Enterprise HR Data Platform</h3>
                  <p className="text-muted-foreground">
                    Developed unified HR data platform integrating 8+ systems, enabling advanced analytics and reducing
                    reporting delays significantly.
                  </p>
                </div>
              </div>
            </div>

            {/* Print Button */}
            <div className="flex justify-end gap-2 pt-4 border-t print:hidden">
              <Button variant="outline" onClick={() => window.print()}>
                <ExternalLink className="mr-2 h-4 w-4" />
                Print Resume
              </Button>
              <Button onClick={() => setIsResumeOpen(false)}>Close</Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
