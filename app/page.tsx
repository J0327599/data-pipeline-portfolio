"use client"

import { DialogDescription } from "@/components/ui/dialog"

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
  Code,
  Cpu,
  Database,
  ExternalLink,
  FileSpreadsheet,
  FileText,
  FolderOpen,
  Home,
  Lightbulb,
  Mail,
  Menu,
  MessageSquare,
  Target,
  TrendingUp,
  Users,
  X,
  BarChart3,
  CheckCircle2,
  Download,
  MapPin,
  Phone,
} from "lucide-react"

// Define ProjectDetail interface
interface ProjectDetail {
  title: string
  problemStatement: string
  architecture: string | undefined
  solution: string | Record<string, string> // Union type to handle both string and object solutions
}

const projectDetails: Record<string, ProjectDetail> = {
  "fraud-detection": {
    title: "Real-time Fraud Detection Pipeline",
    problemStatement:
      "Financial institutions face the challenge of detecting fraudulent transactions in real-time while minimizing false positives that disrupt legitimate customer transactions. Traditional batch processing systems couldn't meet the sub-second response time requirements needed for real-time fraud prevention.",
    architecture: "/real-time-fraud-detection-architecture-diagram-sho.jpg",
    solution: `# Real-time fraud detection using Spark Streaming
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
  },
  "customer-warehouse": {
    title: "Customer Analytics Data Warehouse",
    problemStatement:
      "The marketing team needed a centralized data warehouse to analyze customer behavior across multiple touchpoints (web, mobile, email, social media). Existing data was siloed across different systems, making it impossible to get a unified view of customer journeys and measure marketing campaign effectiveness.",
    architecture: "/data-warehouse-architecture-diagram-showing-etl-pi.jpg",
    solution: `-- Customer 360 view with behavioral analytics
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
  },
  "iot-processing": {
    title: "IoT Sensor Data Processing Platform",
    problemStatement:
      "Manufacturing facilities needed to process millions of IoT sensor readings per hour from industrial equipment to enable predictive maintenance and prevent costly downtime. The existing system couldn't handle the volume and velocity of sensor data, leading to delayed insights and missed maintenance opportunities.",
    architecture: "/iot-data-processing-architecture-with-kafka--spark.jpg",
    solution: {
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

-- Materialized view for recent critical alerts
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
import requests # Added import

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
     * Body: Include error details and record counts`,
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
  "fcrm-reporting": {
    title: "Financial Crime Risk Management BI Platform",
    problemStatement:
      "The Financial Crime Risk Management (FCRM) team within Personal & Private Banking relied on fragmented, manually produced reports scattered across Excel workbooks, static email attachments, and ad-hoc SQL extractions. AML transaction monitoring, fraud detection case outcomes, and suspicious activity reporting (SAR) data lived in separate silos with no unified view. Analysts spent 60%+ of their time on data wrangling rather than insight generation, compliance deadlines were frequently at risk, and leadership lacked real-time visibility into key risk indicators across the financial crime landscape.",
    architecture: "/fcrm-bi-platform-architecture.jpg",
    solution: {
      sql: `-- FCRM Unified Risk Scorecard: AML + Fraud + SAR Analytics
-- SQL Server stored procedure for daily risk dashboard refresh

CREATE PROCEDURE [dbo].[sp_FCRM_DailyRiskScorecard]
    @ReportDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET @ReportDate = ISNULL(@ReportDate, CAST(GETDATE() AS DATE));

    -- 1. AML Transaction Monitoring Summary
    WITH AML_Alerts AS (
        SELECT
            a.alert_id,
            a.customer_id,
            c.customer_segment,
            c.branch_code,
            a.alert_type,
            a.risk_score,
            a.alert_status,
            a.created_date,
            a.resolved_date,
            DATEDIFF(DAY, a.created_date, ISNULL(a.resolved_date, GETDATE())) AS aging_days
        FROM dbo.AML_Alerts a
        INNER JOIN dbo.Customers c ON a.customer_id = c.customer_id
        WHERE a.created_date >= DATEADD(MONTH, -3, @ReportDate)
    ),

    -- 2. Fraud Detection Case Outcomes
    Fraud_Cases AS (
        SELECT
            fc.case_id,
            fc.fraud_type,
            fc.detection_method,
            fc.case_status,
            fc.loss_amount,
            fc.recovered_amount,
            CASE
                WHEN fc.loss_amount > 0
                THEN CAST(fc.recovered_amount AS FLOAT) / fc.loss_amount * 100
                ELSE 0
            END AS recovery_rate_pct,
            fc.reported_date
        FROM dbo.Fraud_Cases fc
        WHERE fc.reported_date >= DATEADD(MONTH, -3, @ReportDate)
    ),

    -- 3. SAR Filing Compliance Tracker
    SAR_Filings AS (
        SELECT
            s.sar_id,
            s.filing_status,
            s.due_date,
            s.submitted_date,
            CASE
                WHEN s.submitted_date <= s.due_date THEN 'On Time'
                WHEN s.submitted_date IS NULL AND s.due_date < @ReportDate THEN 'Overdue'
                ELSE 'Late'
            END AS compliance_status,
            s.risk_category
        FROM dbo.SAR_Filings s
        WHERE s.due_date >= DATEADD(MONTH, -6, @ReportDate)
    )

    -- Final Risk Scorecard Output
    SELECT
        @ReportDate AS report_date,
        -- AML Metrics
        COUNT(DISTINCT aa.alert_id) AS total_aml_alerts,
        SUM(CASE WHEN aa.alert_status = 'Open' THEN 1 ELSE 0 END) AS open_aml_alerts,
        AVG(aa.aging_days) AS avg_alert_aging_days,
        SUM(CASE WHEN aa.risk_score >= 80 THEN 1 ELSE 0 END) AS high_risk_alerts,
        -- Fraud Metrics
        COUNT(DISTINCT frc.case_id) AS total_fraud_cases,
        SUM(frc.loss_amount) AS total_fraud_losses,
        SUM(frc.recovered_amount) AS total_recovered,
        AVG(frc.recovery_rate_pct) AS avg_recovery_rate,
        -- SAR Compliance
        COUNT(DISTINCT sf.sar_id) AS total_sars,
        SUM(CASE WHEN sf.compliance_status = 'On Time' THEN 1 ELSE 0 END) AS sars_on_time,
        SUM(CASE WHEN sf.compliance_status = 'Overdue' THEN 1 ELSE 0 END) AS sars_overdue,
        CAST(SUM(CASE WHEN sf.compliance_status = 'On Time' THEN 1 ELSE 0 END) AS FLOAT)
            / NULLIF(COUNT(sf.sar_id), 0) * 100 AS sar_compliance_rate
    FROM AML_Alerts aa
    CROSS JOIN Fraud_Cases frc
    CROSS JOIN SAR_Filings sf;
END;
GO`,
      python: `# FCRM Automated Report Generator & SSIS Package Trigger
# Automates daily scorecard refresh and distribution

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import openpyxl
from openpyxl.chart import BarChart, PieChart, Reference
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('FCRM_Reporter')

class FCRMReportAutomation:
    def __init__(self):
        self.conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=fcrm-sql-prod.database.windows.net;"
            "Database=FCRM_Analytics;"
            "Authentication=ActiveDirectoryMsi;"
        )

    def execute_ssis_package(self):
        """Trigger SSIS ETL package to refresh staging tables"""
        logger.info("Triggering SSIS package: FCRM_Daily_ETL")
        cmd = [
            "dtexec",
            "/ISServer",
            "\\\\SSISDB\\\\FCRM_Packages\\\\FCRM_Daily_ETL.dtsx",
            "/Server", "fcrm-sql-prod"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("SSIS package executed successfully")
        else:
            logger.error(f"SSIS execution failed: {result.stderr}")
            raise RuntimeError("SSIS package failed")

    def generate_risk_scorecard(self, report_date=None):
        """Generate the FCRM risk scorecard Excel workbook"""
        report_date = report_date or datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Generating FCRM scorecard for {report_date}")

        conn = pyodbc.connect(self.conn_str)

        # Execute stored procedure
        scorecard_df = pd.read_sql(
            "EXEC sp_FCRM_DailyRiskScorecard @ReportDate = ?",
            conn, params=[report_date]
        )

        # AML alerts breakdown
        aml_df = pd.read_sql("""
            SELECT alert_type, alert_status, COUNT(*) as cnt,
                   AVG(risk_score) as avg_risk_score
            FROM dbo.AML_Alerts
            WHERE created_date >= DATEADD(MONTH, -3, ?)
            GROUP BY alert_type, alert_status
            ORDER BY cnt DESC
        """, conn, params=[report_date])

        # Fraud trends by type
        fraud_df = pd.read_sql("""
            SELECT fraud_type, detection_method,
                   COUNT(*) as cases,
                   SUM(loss_amount) as total_loss,
                   SUM(recovered_amount) as total_recovered
            FROM dbo.Fraud_Cases
            WHERE reported_date >= DATEADD(MONTH, -6, ?)
            GROUP BY fraud_type, detection_method
        """, conn, params=[report_date])

        conn.close()

        # Build Excel workbook with charts
        wb = openpyxl.Workbook()
        self._build_executive_summary(wb, scorecard_df)
        self._build_aml_sheet(wb, aml_df)
        self._build_fraud_sheet(wb, fraud_df)

        filename = f"FCRM_Risk_Scorecard_{report_date}.xlsx"
        wb.save(filename)
        logger.info(f"Scorecard saved: {filename}")
        return filename

    def _build_executive_summary(self, wb, df):
        ws = wb.active
        ws.title = "Executive Summary"
        header_fill = PatternFill(start_color="0E7490", fill_type="solid")
        header_font = Font(name="Calibri", bold=True, color="FFFFFF", size=12)

        headers = ["Metric", "Value", "RAG Status"]
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.fill = header_fill
            cell.font = header_font

        row = df.iloc[0]
        metrics = [
            ("Total AML Alerts (3M)", row["total_aml_alerts"], "amber"),
            ("Open AML Alerts", row["open_aml_alerts"], "red"),
            ("Avg Alert Aging (days)", row["avg_alert_aging_days"], "amber"),
            ("High Risk Alerts", row["high_risk_alerts"], "red"),
            ("Total Fraud Cases", row["total_fraud_cases"], "amber"),
            ("SAR Compliance Rate %", row["sar_compliance_rate"], "green"),
        ]
        for i, (metric, value, rag) in enumerate(metrics, 2):
            ws.cell(row=i, column=1, value=metric)
            ws.cell(row=i, column=2, value=value)
            ws.cell(row=i, column=3, value=rag.upper())

    def run_daily_pipeline(self):
        """Full daily pipeline: ETL -> Report -> Distribute"""
        logger.info("Starting FCRM daily reporting pipeline")
        self.execute_ssis_package()
        filename = self.generate_risk_scorecard()
        logger.info("FCRM daily pipeline complete")
        return filename

if __name__ == "__main__":
    pipeline = FCRMReportAutomation()
    pipeline.run_daily_pipeline()`,
      powerbi: `// FCRM Power BI DAX Measures for Risk Dashboard

// 1. AML Alert Volume - Rolling 90 Days
AML Alert Volume 90D =
CALCULATE(
    COUNTROWS('AML_Alerts'),
    DATESINPERIOD(
        'Calendar'[Date],
        MAX('Calendar'[Date]),
        -90,
        DAY
    )
)

// 2. Fraud Recovery Rate KPI
Fraud Recovery Rate =
VAR TotalLoss = SUM('Fraud_Cases'[loss_amount])
VAR TotalRecovered = SUM('Fraud_Cases'[recovered_amount])
RETURN
    DIVIDE(TotalRecovered, TotalLoss, 0) * 100

// 3. SAR Compliance Rate with Conditional Formatting
SAR Compliance Rate =
VAR OnTime =
    CALCULATE(
        COUNTROWS('SAR_Filings'),
        'SAR_Filings'[compliance_status] = "On Time"
    )
VAR Total = COUNTROWS('SAR_Filings')
RETURN
    DIVIDE(OnTime, Total, 0) * 100

// 4. High Risk Customer Count
High Risk Customers =
CALCULATE(
    DISTINCTCOUNT('AML_Alerts'[customer_id]),
    'AML_Alerts'[risk_score] >= 80,
    'AML_Alerts'[alert_status] = "Open"
)

// 5. Alert Aging Buckets for Heatmap
Alert Aging Bucket =
SWITCH(
    TRUE(),
    'AML_Alerts'[aging_days] <= 7, "0-7 Days",
    'AML_Alerts'[aging_days] <= 30, "8-30 Days",
    'AML_Alerts'[aging_days] <= 60, "31-60 Days",
    "60+ Days"
)`,
    },
  },
  "fcrm-data-automation": {
    title: "FCRM Data Extraction & Process Automation Engine",
    problemStatement:
      "Financial Crime Risk Management analysts were spending over 15 hours per week on manual data extractions from Oracle, SQL Server, and SAS datasets to produce compliance reports, ad-hoc investigations, and regulatory submissions. Each extraction involved multiple disconnected queries, manual Excel consolidation, and error-prone copy-paste workflows. The lack of automation led to inconsistent data, missed deadlines, and significant operational risk in a highly regulated environment where accuracy and timeliness are critical for FICA, FIC Act, and SARB compliance.",
    architecture: "/fcrm-data-automation-architecture.jpg",
    solution: {
      sql: `-- Oracle & SQL Server cross-platform extraction for FCRM investigations
-- Unified view combining Oracle transaction data with SQL Server case management

-- Step 1: SQL Server - Create linked server to Oracle
EXEC sp_addlinkedserver
    @server = 'ORACLE_TXN_DB',
    @srvproduct = 'Oracle',
    @provider = 'OraOLEDB.Oracle',
    @datasrc = 'fcrm-oracle-prod.bank.local';

-- Step 2: Investigation Case Extract with Oracle Transaction Join
CREATE PROCEDURE [dbo].[sp_FCRM_InvestigationExtract]
    @InvestigationId INT,
    @DateFrom DATE,
    @DateTo DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- Pull case details from SQL Server case management
    SELECT
        ic.case_id,
        ic.investigation_type,
        ic.suspect_customer_id,
        c.full_name,
        c.id_number,
        c.account_number,
        c.branch_code,
        ic.assigned_analyst,
        ic.priority_level,
        ic.status
    INTO #CaseDetails
    FROM dbo.Investigation_Cases ic
    INNER JOIN dbo.Customers c ON ic.suspect_customer_id = c.customer_id
    WHERE ic.case_id = @InvestigationId;

    -- Pull transaction history from Oracle via linked server
    SELECT
        cd.case_id,
        cd.full_name,
        t.transaction_id,
        t.transaction_date,
        t.transaction_type,
        t.amount,
        t.currency,
        t.originating_account,
        t.beneficiary_account,
        t.beneficiary_name,
        t.swift_code,
        t.country_code,
        -- Flag high-risk jurisdictions
        CASE
            WHEN t.country_code IN ('KP','IR','SY','MM','AF')
            THEN 'HIGH RISK - SANCTIONED'
            WHEN t.country_code IN ('PA','VG','KY','BZ')
            THEN 'ELEVATED - TAX HAVEN'
            ELSE 'STANDARD'
        END AS jurisdiction_risk
    FROM #CaseDetails cd
    INNER JOIN OPENQUERY(ORACLE_TXN_DB,
        'SELECT transaction_id, transaction_date, transaction_type,
                amount, currency, originating_account,
                beneficiary_account, beneficiary_name,
                swift_code, country_code
         FROM CORE_BANKING.TRANSACTIONS
         WHERE transaction_date BETWEEN TO_DATE(''' + CONVERT(VARCHAR, @DateFrom, 23) + ''', ''YYYY-MM-DD'')
         AND TO_DATE(''' + CONVERT(VARCHAR, @DateTo, 23) + ''', ''YYYY-MM-DD'')') t
        ON cd.account_number = t.originating_account;

    -- Structuring detection patterns
    SELECT
        t.originating_account,
        CAST(t.transaction_date AS DATE) AS txn_date,
        COUNT(*) AS daily_txn_count,
        SUM(t.amount) AS daily_total,
        -- Structuring detection: multiple transactions just below threshold
        SUM(CASE WHEN t.amount BETWEEN 20000 AND 24999 THEN 1 ELSE 0 END)
            AS near_threshold_count,
        CASE
            WHEN COUNT(*) >= 5 AND SUM(t.amount) > 100000 THEN 'STRUCTURING SUSPECTED'
            WHEN SUM(CASE WHEN t.amount BETWEEN 20000 AND 24999 THEN 1 ELSE 0 END) >= 3
            THEN 'SMURFING PATTERN'
            ELSE 'NORMAL'
        END AS pattern_flag
    FROM #CaseDetails cd
    INNER JOIN OPENQUERY(ORACLE_TXN_DB,
        'SELECT * FROM CORE_BANKING.TRANSACTIONS') t
        ON cd.account_number = t.originating_account
    GROUP BY t.originating_account, CAST(t.transaction_date AS DATE)
    HAVING COUNT(*) >= 3 OR SUM(t.amount) > 50000
    ORDER BY daily_total DESC;

    DROP TABLE #CaseDetails;
END;
GO`,
      python: `# FCRM Process Automation Engine
# Automates data extraction, SAS dataset conversion, and report distribution

import pyodbc
import cx_Oracle
import pandas as pd
import sas7bdat
from datetime import datetime, timedelta
from pathlib import Path
import schedule
import logging
import json
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('FCRM_Automation')

class FCRMDataAutomation:
    def __init__(self, config_path='fcrm_config.json'):
        with open(config_path) as f:
            self.config = json.load(f)

        self.sql_conn = pyodbc.connect(
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=fcrm-sql-prod.bank.local;"
            "Database=FCRM_CaseManagement;"
            "Trusted_Connection=yes;"
        )
        self.oracle_conn = cx_Oracle.connect(
            "fcrm_read/****@fcrm-oracle-prod.bank.local:1521/COREBANK"
        )

    def extract_sas_datasets(self, sas_directory):
        """Convert SAS7BDAT files to pandas DataFrames"""
        logger.info(f"Processing SAS datasets from {sas_directory}")
        sas_path = Path(sas_directory)
        datasets = {}

        for sas_file in sas_path.glob("*.sas7bdat"):
            logger.info(f"Reading SAS file: {sas_file.name}")
            with sas7bdat.SAS7BDAT(str(sas_file)) as reader:
                df = reader.to_data_frame()
                datasets[sas_file.stem] = df
                logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

        return datasets

    def run_aml_threshold_report(self):
        """Generate automated AML threshold monitoring report"""
        logger.info("Running AML threshold monitoring extraction")

        # Extract from Oracle core banking
        oracle_query = """
            SELECT
                t.account_number,
                t.customer_id,
                c.full_name,
                c.id_type,
                c.id_number,
                COUNT(*) as txn_count,
                SUM(t.amount) as total_amount,
                MAX(t.amount) as max_single_txn,
                COUNT(DISTINCT t.beneficiary_account) as unique_beneficiaries,
                COUNT(DISTINCT t.country_code) as unique_countries
            FROM CORE_BANKING.TRANSACTIONS t
            JOIN CORE_BANKING.CUSTOMERS c ON t.customer_id = c.customer_id
            WHERE t.transaction_date >= TRUNC(SYSDATE) - 30
            GROUP BY t.account_number, t.customer_id,
                     c.full_name, c.id_type, c.id_number
            HAVING SUM(t.amount) > 25000
               OR COUNT(*) > 20
               OR COUNT(DISTINCT t.country_code) > 3
            ORDER BY total_amount DESC
        """
        oracle_df = pd.read_sql(oracle_query, self.oracle_conn)

        # Cross-reference with SQL Server watchlists
        watchlist_query = """
            SELECT customer_id, watchlist_type, match_score,
                   listed_date, source_list
            FROM dbo.Watchlist_Matches
            WHERE is_active = 1
        """
        watchlist_df = pd.read_sql(watchlist_query, self.sql_conn)

        # Merge and flag
        merged = oracle_df.merge(
            watchlist_df, on='customer_id', how='left'
        )
        merged['risk_flag'] = merged.apply(self._calculate_risk_flag, axis=1)
        merged['report_date'] = datetime.now().strftime('%Y-%m-%d')

        # Generate Excel report
        filename = self._create_aml_workbook(merged)
        logger.info(f"AML threshold report saved: {filename}")
        return filename

    def _calculate_risk_flag(self, row):
        score = 0
        if row['total_amount'] > 100000: score += 30
        if row['txn_count'] > 50: score += 20
        if row['unique_countries'] > 5: score += 25
        if pd.notna(row.get('watchlist_type')): score += 40
        if row.get('match_score', 0) > 0.8: score += 20

        if score >= 70: return 'CRITICAL'
        elif score >= 40: return 'HIGH'
        elif score >= 20: return 'MEDIUM'
        return 'LOW'

    def _create_aml_workbook(self, df):
        wb = Workbook()
        ws = wb.active
        ws.title = "AML Threshold Report"

        # Header styling
        header_fill = PatternFill(start_color="0E7490", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF", size=11)

        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)

        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font

        filename = f"AML_Threshold_Report_{datetime.now():%Y%m%d}.xlsx"
        wb.save(filename)
        return filename

    def schedule_daily_runs(self):
        """Schedule automated daily extractions"""
        schedule.every().day.at("06:00").do(self.run_aml_threshold_report)
        logger.info("FCRM automation scheduler started")
        while True:
            schedule.run_pending()

if __name__ == "__main__":
    engine = FCRMDataAutomation()
    engine.run_aml_threshold_report()`,
      qlikview: `// QlikView Load Script for FCRM Dashboard
// Connects to SQL Server FCRM database and builds associative data model

SET ThousandSep=',';
SET DecimalSep='.';
SET MoneyThousandSep=',';
SET MoneyFormat='R #,##0.00';
SET TimeFormat='hh:mm:ss';
SET DateFormat='YYYY-MM-DD';

// ============================
// AML ALERTS FACT TABLE
// ============================
AML_Alerts:
LOAD
    alert_id,
    customer_id,
    alert_type,
    risk_score,
    alert_status,
    created_date AS alert_created_date,
    resolved_date AS alert_resolved_date,
    assigned_analyst,
    INTERVAL(resolved_date - created_date, 'D') AS resolution_days,
    IF(risk_score >= 80, 'Critical',
       IF(risk_score >= 60, 'High',
          IF(risk_score >= 40, 'Medium', 'Low'))) AS risk_category,
    Month(created_date) AS alert_month,
    Year(created_date) AS alert_year;
SQL SELECT * FROM dbo.AML_Alerts
    WHERE created_date >= DATEADD(YEAR, -1, GETDATE());

// ============================
// FRAUD CASES FACT TABLE
// ============================
Fraud_Cases:
LOAD
    case_id,
    customer_id,
    fraud_type,
    detection_method,
    case_status,
    loss_amount,
    recovered_amount,
    reported_date,
    loss_amount - recovered_amount AS net_loss,
    IF(loss_amount > 0,
       recovered_amount / loss_amount * 100, 0) AS recovery_rate,
    Month(reported_date) AS fraud_month,
    Year(reported_date) AS fraud_year;
SQL SELECT * FROM dbo.Fraud_Cases
    WHERE reported_date >= DATEADD(YEAR, -1, GETDATE());

// ============================
// CUSTOMER DIMENSION
// ============================
Customers:
LOAD
    customer_id,
    full_name,
    customer_segment,
    branch_code,
    province,
    account_type,
    onboarding_date,
    kyc_status,
    pep_flag;
SQL SELECT * FROM dbo.Customers;

// ============================
// EXPRESSIONS FOR DASHBOARD
// ============================
// Total Open Alerts: =COUNT({<alert_status={'Open'}>} alert_id)
// Avg Resolution Days: =AVG(resolution_days)
// Fraud Loss Ratio: =SUM(net_loss) / SUM(loss_amount) * 100
// SAR Compliance: =COUNT({<compliance_status={'On Time'}>} sar_id) / COUNT(sar_id) * 100`,
    },
  },
}

const downloadCV = () => {
  const printWindow = window.open("", "_blank")
  if (!printWindow) return

  printWindow.document.write(`
    <!DOCTYPE html>
    <html>
      <head>
        <title>Stanton Edwards - CV</title>
        <style>
          * { margin: 0; padding: 0; box-sizing: border-box; }
          body { 
            font-family: 'Arial', sans-serif; 
            line-height: 1.6; 
            color: #1a202c;
            padding: 40px;
            max-width: 210mm;
            margin: 0 auto;
          }
          h1 { 
            font-size: 32px; 
            color: #0e7490; 
            margin-bottom: 8px;
            border-bottom: 3px solid #84cc16;
            padding-bottom: 10px;
          }
          h2 { 
            font-size: 20px; 
            color: #0e7490; 
            margin-top: 24px;
            margin-bottom: 12px;
            border-bottom: 2px solid #e2e8f0;
            padding-bottom: 6px;
          }
          h3 { 
            font-size: 16px; 
            color: #1e40af; 
            margin-top: 16px;
            margin-bottom: 8px;
          }
          .header { 
            text-align: center; 
            margin-bottom: 30px;
          }
          .contact-info { 
            display: flex; 
            justify-content: center; 
            gap: 20px; 
            flex-wrap: wrap;
            margin-top: 12px;
            font-size: 14px;
          }
          .section { 
            margin-bottom: 24px;
            page-break-inside: avoid;
          }
          .job { 
            margin-bottom: 20px;
            page-break-inside: avoid;
          }
          .job-header { 
            display: flex; 
            justify-content: space-between; 
            margin-bottom: 8px;
          }
          .job-title { 
            font-weight: bold; 
            color: #1e40af;
            font-size: 15px;
          }
          .company { 
            color: #0e7490; 
            font-weight: 600;
          }
          .duration { 
            color: #64748b; 
            font-style: italic;
            font-size: 14px;
          }
          ul { 
            margin-left: 20px; 
            margin-top: 8px;
          }
          li { 
            margin-bottom: 6px;
            font-size: 14px;
          }
          .skills-grid { 
            display: grid; 
            grid-template-columns: repeat(2, 1fr); 
            gap: 16px;
            margin-top: 12px;
          }
          .skill-category { 
            margin-bottom: 12px;
          }
          .skill-category strong { 
            color: #1e40af;
            display: block;
            margin-bottom: 6px;
          }
          .badges { 
            display: flex; 
            flex-wrap: wrap; 
            gap: 8px;
            margin-top: 6px;
          }
          .badge { 
            background: #e0f2fe; 
            color: #0369a1; 
            padding: 4px 12px; 
            border-radius: 12px; 
            font-size: 12px;
            font-weight: 500;
          }
          @media print {
            body { padding: 20px; }
            .section { page-break-inside: avoid; }
          }
        </style>
      </head>
      <body>
        <div class="header">
          <h1>STANTON EDWARDS</h1>
          <p style="font-size: 16px; color: #0e7490; font-weight: 600;">BI Manager | Big Data Enthusiast | Analytics Expert | Senior Data Engineer</p>
          <div class="contact-info">
            <span>📧 stanton.edwards@outlook.com</span>
            <span>📱 079 881 0997</span>
            <span>📍 Johannesburg, South Africa</span>
          </div>
        </div>

        <div class="section">
          <h2>PROFESSIONAL SUMMARY</h2>
          <p>Results-driven Data Analytics and AI leader with 10+ years of experience delivering enterprise-scale analytics solutions and AI/ML platforms in the energy and financial services sectors. Proven track record of building high-performing teams, implementing real-time analytics systems, and driving measurable business value through data-driven insights. Expert in Python, SQL, Spark, Power BI, and cloud platforms (Azure, AWS). Strong business acumen with ability to translate complex technical concepts into strategic recommendations for C-level executives.</p>
        </div>

        <div class="section">
          <h2>PROFESSIONAL EXPERIENCE</h2>
          
          <div class="job">
            <div class="job-header">
              <div>
                <div class="job-title">Data Analytics & AI Lead</div>
                <div class="company">TotalEnergies • Finance & IS Management • Rosebank Johannesburg</div>
              </div>
              <div class="duration">Current Role</div>
            </div>
            <ul>
              <li>Lead a team of 12+ data scientists, analysts, and ML engineers across advanced analytics, AI/ML, and business intelligence functions</li>
              <li>Developed and executed enterprise analytics strategy aligned with digital transformation goals, delivering R150M+ in measurable business value</li>
              <li>Built customer insights and personalization platform using ML/NLP, increasing customer retention by 28% and cross-sell conversion by 35%</li>
              <li>Implemented real-time risk analytics and automated decision-making systems processing 5M+ transactions daily with 99.2% accuracy</li>
              <li>Established AI ethics framework and governance policies ensuring fairness, transparency, and POPIA compliance</li>
              <li>Championed data-driven culture through executive dashboards (Power BI) and self-service analytics, enabling 500+ business users</li>
              <li>Led predictive analytics initiatives for insurance underwriting and claims optimization, reducing loss ratios by 18%</li>
            </ul>
          </div>

          <div class="job">
            <div class="job-header">
              <div>
                <div class="job-title">BI Solutions Architect - Data & Analytics</div>
                <div class="company">TotalEnergies • Finance & IS • Rosebank Johannesburg</div>
              </div>
              <div class="duration">2022 - 2024</div>
            </div>
            <ul>
              <li>Designed and implemented enterprise BI architecture supporting 1000+ users across multiple business units</li>
              <li>Led migration from legacy reporting systems to modern cloud-based analytics platform (Azure Synapse + Power BI)</li>
              <li>Developed data governance framework and metadata management strategy ensuring data quality and compliance</li>
              <li>Built real-time operational dashboards reducing decision-making time from days to minutes</li>
              <li>Established center of excellence for analytics, providing training and best practices to 200+ users</li>
            </ul>
          </div>

          <div class="job">
            <div class="job-header">
              <div>
                <div class="job-title">Senior Data Engineer & Analytics Lead</div>
                <div class="company">Retail and B2B • TotalEnergies</div>
              </div>
              <div class="duration">2019 - 2022 • 3 Years</div>
            </div>
            <ul>
              <li>Led analytics team of 8 engineers delivering advanced analytics solutions and ML model deployment</li>
              <li>Conducted comprehensive business requirements analysis for 15+ analytics projects, translating stakeholder needs into technical specifications</li>
              <li>Facilitated cross-functional workshops with business stakeholders to define KPIs, success metrics, and reporting requirements</li>
              <li>Developed detailed functional specifications, user stories, and acceptance criteria for analytics platform enhancements</li>
              <li>Built customer segmentation and propensity models using Python/R, driving 45% improvement in marketing campaign ROI and 32% increase in customer lifetime value</li>
              <li>Performed cost-benefit analysis for analytics investments, demonstrating R12M annual savings through process optimization</li>
              <li>Created data-driven business cases that secured R25M in funding for customer analytics platform expansion</li>
              <li>Optimized Spark jobs reducing processing time by 70% and infrastructure costs by 45%</li>
              <li>Implemented real-time streaming analytics processing 50M+ events daily with sub-second latency</li>
              <li>Established data quality frameworks and automated testing, reducing data incidents by 85%</li>
            </ul>
          </div>

          <div class="job">
            <div class="job-header">
              <div>
                <div class="job-title">Data Engineer & Business Analyst</div>
                <div class="company">DataFlow Analytics</div>
              </div>
              <div class="duration">2016 - 2019 • 3 Years</div>
            </div>
            <ul>
              <li>Elicited and documented business requirements through stakeholder interviews, surveys, and process mapping sessions</li>
              <li>Created comprehensive process flow diagrams, data flow diagrams, and business process models using BPMN notation</li>
              <li>Conducted gap analysis between current state and desired future state, identifying improvement opportunities worth R8M annually</li>
              <li>Developed business requirement documents (BRDs) and functional requirement documents (FRDs) for 20+ data projects</li>
              <li>Facilitated UAT sessions with business users, managing feedback incorporation and sign-off processes</li>
              <li>Built and maintained ETL pipelines processing 100GB+ daily using Apache Airflow and Python</li>
              <li>Developed statistical models in Python/R for customer behavior analysis and churn prediction</li>
              <li>Created executive dashboards in Tableau combining complex data signals into actionable insights</li>
              <li>Collaborated with data scientist to productionize ML models serving 1M+ predictions daily</li>
              <li>Performed root cause analysis on data quality issues, implementing fixes that improved accuracy by 95%</li>
            </ul>
          </div>
        </div>

        <div class="section">
          <h2>TECHNICAL SKILLS</h2>
          <div class="skills-grid">
            <div class="skill-category">
              <strong>Programming & Scripting:</strong>
              <div class="badges">
                <span class="badge">Python</span>
                <span class="badge">SQL</span>
                <span class="badge">R</span>
                <span class="badge">Scala</span>
                <span class="badge">VBA</span>
              </div>
            </div>
            <div class="skill-category">
              <strong>Big Data & Processing:</strong>
              <div class="badges">
                <span class="badge">Apache Spark</span>
                <span class="badge">Hadoop</span>
                <span class="badge">Kafka</span>
                <span class="badge">Airflow</span>
                <span class="badge">Alteryx</span>
              </div>
            </div>
            <div class="skill-category">
              <strong>Cloud Platforms:</strong>
              <div class="badges">
                <span class="badge">Azure</span>
                <span class="badge">AWS</span>
                <span class="badge">GCP</span>
                <span class="badge">Databricks</span>
              </div>
            </div>
            <div class="skill-category">
              <strong>Databases:</strong>
              <div class="badges">
                <span class="badge">PostgreSQL</span>
                <span class="badge">MySQL</span>
                <span class="badge">MongoDB</span>
                <span class="badge">Cassandra</span>
                <span class="badge">Snowflake</span>
              </div>
            </div>
            <div class="skill-category">
              <strong>Analytics & BI:</strong>
              <div class="badges">
                <span class="badge">Power BI</span>
                <span class="badge">Tableau</span>
                <span class="badge">Looker</span>
                <span class="badge">Excel</span>
                <span class="badge">MS Access</span>
              </div>
            </div>
            <div class="skill-category">
              <strong>ML & AI:</strong>
              <div class="badges">
                <span class="badge">TensorFlow</span>
                <span class="badge">PyTorch</span>
                <span class="badge">Scikit-learn</span>
                <span class="badge">NLP</span>
                <span class="badge">Computer Vision</span>
              </div>
            </div>
          </div>
        </div>

        <div class="section">
          <h2>EDUCATION</h2>
          <div class="job">
            <div class="job-header">
              <div>
                <div class="job-title">Master of Science in Data Science</div>
                <div class="company">University of Johannesburg</div>
              </div>
              <div class="duration">2018</div>
            </div>
          </div>
          <div class="job">
            <div class="job-header">
              <div>
                <div class="job-title">Bachelor of Science in Computer Science</div>
                <div class="company">University of the Witwatersrand</div>
              </div>
              <div class="duration">2014</div>
            </div>
          </div>
        </div>

        <div class="section">
          <h2>CERTIFICATIONS</h2>
          <ul>
            <li>AWS Certified Solutions Architect - Professional</li>
            <li>Microsoft Azure Data Engineer Associate</li>
            <li>Google Cloud Professional Data Engineer</li>
            <li>Databricks Certified Data Engineer Professional</li>
            <li>TOGAF 9 Certified</li>
          </ul>
        </div>

        <div class="section">
          <h2>KEY PROJECTS</h2>
          <div class="job">
            <h3>Insurance Analytics & Insights Platform</h3>
            <p><strong>Technologies:</strong> SQL, Python, Power BI, Azure Synapse</p>
            <p>Built comprehensive insurance analytics platform processing 500K+ policies and 2M+ claims annually. Implemented predictive models for claims forecasting and fraud detection, delivering R45M in cost savings.</p>
          </div>
          <div class="job">
            <h3>Customer Analytics Data Warehouse</h3>
            <p><strong>Technologies:</strong> SQL, Python, Redshift, dbt</p>
            <p>Designed and implemented enterprise customer 360 data warehouse integrating 12 data sources. Enabled marketing team to achieve 45% improvement in campaign ROI through advanced segmentation.</p>
          </div>
          <div class="job">
            <h3>Real-time Fraud Detection Pipeline</h3>
            <p><strong>Technologies:</strong> Python, Spark Streaming, Kafka, AWS</p>
            <p>Built real-time fraud detection system processing 5M+ transactions daily with sub-second latency. Achieved 95% fraud detection rate while reducing false positives by 60%.</p>
          </div>
        </div>
      </body>
    </html>
  `)

  printWindow.document.close()
  printWindow.focus()

  setTimeout(() => {
    printWindow.print()
  }, 250)
}

const Portfolio = () => {
  const [selectedProject, setSelectedProject] = useState<string | null>(null)
  const [isResumeOpen, setIsResumeOpen] = useState(false)
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false) // Renamed state variable

  // const downloadCV = () => {
  //   // Create a new window with the CV content
  //   const cvWindow = window.open("", "_blank")
  //   if (!cvWindow) return

  //   // Generate comprehensive CV HTML
  //   const cvHTML = `
  // <!DOCTYPE html>
  // <html>
  // <head>
  //   <meta charset="UTF-8">
  //   <title>Stanton Edwards - CV</title>
  //   <style>
  //     * { margin: 0; padding: 0; box-sizing: border-box; }
  //     body {
  //       font-family: 'Arial', sans-serif;
  //       line-height: 1.6;
  //       color: #333;
  //       padding: 40px;
  //       max-width: 210mm;
  //       margin: 0 auto;
  //     }
  //     h1 {
  //       color: #1a365d;
  //       font-size: 32px;
  //       margin-bottom: 10px;
  //       border-bottom: 3px solid #1a365d;
  //       padding-bottom: 10px;
  //     }
  //     h2 {
  //       color: #1a365d;
  //       font-size: 20px;
  //       margin-top: 25px;
  //       margin-bottom: 15px;
  //       border-bottom: 2px solid #e2e8f0;
  //       padding-bottom: 5px;
  //     }
  //     h3 {
  //       color: #2d3748;
  //       font-size: 16px;
  //       margin-top: 15px;
  //       margin-bottom: 8px;
  //     }
  //     .header {
  //       text-align: center;
  //       margin-bottom: 30px;
  //     }
  //     .subtitle {
  //       color: #4a5568;
  //       font-size: 16px;
  //       margin-bottom: 15px;
  //     }
  //     .contact-info {
  //       display: flex;
  //       justify-content: center;
  //       gap: 20px;
  //       flex-wrap: wrap;
  //       margin-bottom: 20px;
  //       font-size: 14px;
  //     }
  //     .contact-item {
  //       display: flex;
  //       align-items: center;
  //       gap: 5px;
  //     }
  //     .summary {
  //       background: #f7fafc;
  //       padding: 15px;
  //       border-left: 4px solid #1a365d;
  //       margin-bottom: 25px;
  //       font-size: 14px;
  //     }
  //     .experience-item, .project-item {
  //       margin-bottom: 20px;
  //       page-break-inside: avoid;
  //     }
  //     .experience-header {
  //       display: flex;
  //       justify-content: space-between;
  //       align-items: flex-start;
  //       margin-bottom: 8px;
  //     }
  //     .job-title {
  //       font-weight: bold;
  //       font-size: 16px;
  //       color: #1a365d;
  //     }
  //     .company {
  //       color: #4a5568;
  //       font-size: 14px;
  //     }
  //     .duration {
  //       color: #718096;
  //       font-size: 13px;
  //       white-space: nowrap;
  //     }
  //     ul {
  //       margin-left: 20px;
  //       margin-top: 8px;
  //     }
  //     li {
  //       margin-bottom: 6px;
  //       font-size: 13px;
  //       line-height: 1.5;
  //     }
  //     .skills-grid {
  //       display: grid;
  //       grid-template-columns: repeat(2, 1fr);
  //       gap: 15px;
  //       margin-top: 15px;
  //     }
  //     .skill-category {
  //       background: #f7fafc;
  //       padding: 12px;
  //       border-radius: 5px;
  //     }
  //     .skill-category h4 {
  //       color: #1a365d;
  //       margin-bottom: 8px;
  //       font-size: 14px;
  //     }
  //     .badge {
  //       display: inline-block;
  //       background: #e2e8f0;
  //       padding: 3px 8px;
  //       border-radius: 3px;
  //       font-size: 11px;
  //       margin-right: 5px;
  //       margin-bottom: 5px;
  //     }
  //     .project-item {
  //       background: #f7fafc;
  //       padding: 15px;
  //       border-radius: 5px;
  //       margin-bottom: 15px;
  //     }
  //     @media print {
  //       body { padding: 20px; }
  //       .page-break { page-break-before: always; }
  //     }
  //   </style>
  // </head>
  // <body>
  //   <div class="header">
  //     <h1>STANTON EDWARDS</h1>
  //     <p class="subtitle">BI Manager | Big Data Enthusiast | Analytics Expert | Senior Data Engineer</p>
  //     <div class="contact-info">
  //       <div class="contact-item">📧 stanton.edwards@outlook.com</div>
  //       <div class="contact-item">📱 079 881 0997</div>
  //       <div class="contact-item">📍 Johannesburg, South Africa</div>
  //     </div>
  //   </div>

  //   <div class="summary">
  //     <strong>Professional Summary:</strong> Transforming raw data into actionable business insights through scalable infrastructure, advanced analytics, and cutting-edge big data technologies. Over 10 years of experience leading data science teams, developing ML models, and implementing enterprise analytics solutions across financial services and energy sectors.
  //   </div>

  //   <h2>PROFESSIONAL EXPERIENCE</h2>

  //   <div class="experience-item">
  //     <div class="experience-header">
  //       <div>
  //         <div class="job-title">Data Analytics & AI Lead</div>
  //         <div class="company">TotalEnergies • Finance & IS Management • Rosebank Johannesburg</div>
  //       </div>
  //       <div class="duration">2024 - Present</div>
  //     </div>
  //     <ul>
  //       <li>Lead a team of 12 data scientists, analysts, and ML engineers across advanced analytics, AI/ML, and business intelligence functions supporting TotalEnergies' energy transition strategy</li>
  //       <li>Developed and executed enterprise analytics strategy aligned with TotalEnergies' digital transformation initiatives, delivering €120M+ in measurable business value across retail, commercial, and trading operations</li>
  //       <li>Built best-in-class customer insights and personalization platform using ML/NLP for fuel retail operations, increasing customer retention by 28% and cross-sell conversion by 35%</li>
  //       <li>Implemented real-time risk analytics and automated decision-making systems for commodity trading, processing 5M+ transactions daily with 99.2% accuracy</li>
  //       <li>Established AI ethics framework and governance policies ensuring fairness, transparency, and POPIA compliance across all ML models</li>
  //       <li>Championed data-driven culture through Power BI dashboards and self-service analytics, enabling 500+ business users across finance, operations, and commercial functions</li>
  //       <li>Led predictive analytics initiatives for energy demand forecasting and pricing optimization, reducing forecast errors by 22% and improving margin optimization by 18%</li>
  //     </ul>
  //   </div>

  //   <div class="experience-item">
  //     <div class="experience-header">
  //       <div>
  //         <div class="job-title">BI Solutions Architect - Data & Analytics</div>
  //         <div class="company">TotalEnergies • Retail and B2B • Rosebank Johannesburg</div>
  //       </div>
  //       <div class="duration">2022 - 2024</div>
  //     </div>
  //     <ul>
  //       <li>Designed scalable, secure, and high-performance data solutions aligned with business requirements and digital transformation initiatives</li>
  //       <li>Architected cloud-native data solutions across Azure, AWS, and GCP, implementing data lakes, warehouses, and lakehouses</li>
  //       <li>Developed conceptual, logical, and physical data models ensuring data consistency, quality, and lineage across enterprise systems</li>
  //       <li>Embedded data governance principles and ensured compliance with POPIA and GDPR regulations through robust security controls</li>
  //       <li>Led technical architecture reviews and mentored data engineering teams on best practices using TOGAF frameworks</li>
  //     </ul>
  //   </div>

  //   <div class="experience-item">
  //     <div class="experience-header">
  //       <div>
  //         <div class="job-title">Senior Data Engineer & Analytics Lead</div>
  //         <div class="company">TotalEnergies • Retail and B2B</div>
  //       </div>
  //       <div class="duration">2019 - 2022</div>
  //     </div>
  //     <ul>
  //       <li>Led analytics team of 8 engineers delivering advanced analytics solutions and ML model deployment for retail fuel stations and B2B energy clients</li>
  //       <li>Built customer segmentation and propensity models using Python/R, driving 45% improvement in marketing campaign ROI and increasing customer lifetime value by 32%</li>
  //       <li>Developed customer churn prediction models identifying at-risk accounts, enabling proactive retention strategies that reduced B2B customer attrition by 28%</li>
  //       <li>Implemented personalized pricing engine for commercial clients based on consumption patterns, improving customer satisfaction scores by 40% while maintaining margins</li>
  //       <li>Optimized Spark jobs reducing processing time by 70% and infrastructure costs by 45%</li>
  //       <li>Implemented real-time streaming analytics processing 50M+ events daily with sub-second latency for fuel station transactions and loyalty programs</li>
  //       <li>Established data quality frameworks and automated testing, reducing data incidents by 85%</li>
  //     </ul>
  //   </div>

  //   <div class="page-break"></div>

  //   <div class="experience-item">
  //     <div class="experience-header">
  //       <div>
  //         <div class="job-title">Data Engineer & Business Analyst</div>
  //         <div class="company">DataFlow Analytics</div>
  //       </div>
  //       <div class="duration">2016 - 2019</div>
  //     </div>
  //     <ul>
  //       <li>Led requirements gathering and stakeholder engagement sessions with C-level executives, translating complex business needs into technical solutions and data strategies</li>
  //       <li>Conducted comprehensive process analysis and mapping, identifying bottlenecks and optimization opportunities that improved operational efficiency by 35%</li>
  //       <li>Built and maintained ETL pipelines processing 100GB+ daily using Apache Airflow and Python</li>
  //       <li>Developed statistical models in Python/R for customer behavior analysis and churn prediction, providing actionable insights that informed strategic business decisions</li>
  //       <li>Created executive dashboards in Tableau combining complex data signals into actionable insights, facilitating data-driven decision-making across multiple business units</li>
  //       <li>Performed cost-benefit analysis and ROI modeling for proposed initiatives, ensuring alignment with business objectives and optimal resource allocation</li>
  //       <li>Collaborated with data scientist to productionize ML models serving 1M+ predictions daily</li>
  //     </ul>
  //   </div>

  //   <h2>TECHNICAL SKILLS</h2>
  //   <div class="skills-grid">
  //     <div class="skill-category">
  //       <h4>AI & Machine Learning</h4>
  //       <div class="skill-list">
  //         • Machine Learning & Deep Learning<br>
  //         • Natural Language Processing<br>
  //         • Predictive Analytics<br>
  //         • TensorFlow, PyTorch, Scikit-learn
  //       </div>
  //     </div>
  //     <div class="skill-category">
  //       <h4>Analytics & Visualization</h4>
  //       <div class="skill-list">
  //         • Power BI & Tableau<br>
  //         • Advanced Excel & Looker<br>
  //         • Statistical Analysis (SAS/R)<br>
  //         • Data Storytelling
  //       </div>
  //     </div>
  //     <div class="skill-category">
  //       <h4>Big Data Technologies</h4>
  //       <div class="skill-list">
  //         • Apache Spark & Hadoop<br>
  //         • Kafka & Real-time Streaming<br>
  //         • Databricks & EMR<br>
  //         • Data Lakehouse Architecture
  //       </div>
  //     </div>
  //     <div class="skill-category">
  //       <h4>Cloud Computing</h4>
  //       <div class="skill-list">
  //         • AWS (EC2, EMR, Redshift, SageMaker)<br>
  //         • Azure (ML, Data Factory, Synapse)<br>
  //         • GCP (BigQuery, AI Platform)<br>
  //         • Cloud Architecture Design
  //       </div>
  //     </div>
  //     <div class="skill-category">
  //       <h4>Programming Languages</h4>
  //       <div class="skill-list">
  //         • Python (Pandas, NumPy, Scikit-learn)<br>
  //         • R (Statistical Modeling)<br>
  //         • SQL & SAS<br>
  //         • Scala & Java
  //       </div>
  //     </div>
  //     <div class="skill-category">
  //       <h4>Leadership & Strategy</h4>
  //       <div class="skill-list">
  //         • Team Leadership & Mentoring<br>
  //         • Analytics Strategy Development<br>
  //         • Stakeholder Management<br>
  //         • Agile & Scrum Methodologies
  //       </div>
  //     </div>
  //   </div>

  //   <h2>FEATURED PROJECTS</h2>

  //   <div class="project-item">
  //     <h3>Credit Lifecycle BI & Reporting Platform</h3>
  //     <div><span class="badge">SQL Server</span><span class="badge">Python</span><span class="badge">MS Access</span><span class="badge">PowerPoint VBA</span></div>
  //     <p>Built comprehensive BI platform for Business & Commercial Banking, automating credit lifecycle reporting and reducing report generation time from 7 days to 1 day. Integrated data from multiple systems for executive presentations.</p>
  //   </div>

  //   <div class="project-item">
  //     <h3>Enterprise HR Data Platform</h3>
  //     <div><span class="badge">Azure SQL</span><span class="badge">Alteryx</span><span class="badge">Python</span><span class="badge">Power BI</span></div>
  //     <p>Designed and implemented medallion architecture (Bronze/Silver/Gold) for HR data platform, integrating multiple HRIS and payroll systems. Automated workflows with Alteryx, enabling real-time workforce analytics.</p>
  //   </div>

  //   <div class="project-item">
  //     <h3>Insurance Analytics Platform</h3>
  //     <div><span class="badge">SQL</span><span class="badge">Python</span><span class="badge">Power BI</span><span class="badge">ML</span></div>
  //     <p>Improved underwriting efficiency by 40% and reduced loss ratios by 18% through predictive analytics.</p>
  //   </div>

  //   <div class="project-item">
  //     <h3>Real-time Fraud Detection Pipeline</h3>
  //     <div><span class="badge">Kafka</span><span class="badge">Spark</span><span class="badge">Python</span><span class="badge">AWS</span></div>
  //     <p>Processed 5M+ transactions daily with 99.2% accuracy reducing false positives by 60%.</p>
  //   </div>

  //   <h2>EDUCATION</h2>
  //   <div class="experience-item">
  //     <div class="experience-header">
  //       <div>
  //         <div class="job-title">Bachelor of Science in Computer Science & Statistics</div>
  //         <div class="company">University of Johannesburg</div>
  //       </div>
  //       <div class="duration">2013 - 2016</div>
  //     </div>
  //     <p>Specialization in Data Science, Machine Learning, and Statistical Analysis</p>
  //   </div>

  //   <h2>CERTIFICATIONS</h2>
  //   <ul>
  //     <li>AWS Certified Solutions Architect - Professional</li>
  //     <li>Microsoft Certified: Azure Data Engineer Associate</li>
  //     <li>Google Cloud Professional Data Engineer</li>
  //     <li>TOGAF 9 Certified</li>
  //     <li>Certified Analytics Professional (CAP)</li>
  //   </ul>

  // </body>
  // </html>
  //   `

  //   cvWindow.document.write(cvHTML)
  //   cvWindow.document.close()

  //   // Wait for content to load, then trigger print dialog
  //   setTimeout(() => {
  //     cvWindow.print()
  //   }, 500)
  // }

  const scrollToSection = (sectionId: string) => {
    const element = document.getElementById(sectionId)
    if (element) {
      element.scrollIntoView({ behavior: "smooth" })
      setIsMobileMenuOpen(false) // Close mobile menu after navigation
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
                onClick={() => scrollToSection("powerbi")}
                className="flex items-center gap-2 text-sm hover:text-primary transition-colors"
              >
                <BarChart3 className="h-4 w-4" />
                Power BI
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
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
              className="md:hidden p-2 hover:bg-muted rounded-lg transition-colors"
            >
              {isMobileMenuOpen ? <X className="h-6 w-6" /> : <Menu className="h-6 w-6" />}
            </button>
          </div>

          {/* Mobile Navigation */}
          {isMobileMenuOpen && (
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
                onClick={() => scrollToSection("powerbi")}
                className="flex items-center gap-3 w-full px-4 py-3 hover:bg-muted rounded-lg transition-colors"
              >
                <BarChart3 className="h-5 w-5" />
                <span>Power BI</span>
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
            BI Manager | Big Data Enthusiast | Analytics Expert | Senior Data Engineer
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
            <Button variant="outline" size="lg" className="bg-transparent" onClick={downloadCV}>
              <Download className="h-5 w-5 mr-2" />
              Download CV
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

            <Card className="hover:shadow-lg transition-shadow border-primary/20">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <BarChart3 className="h-8 w-8 text-primary" />
                  <CardTitle>Power BI & Visualization</CardTitle>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Power BI (DAX, Power Query, M)</span>
                      <span className="text-sm text-muted-foreground">98%</span>
                    </div>
                    <Progress value={98} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Power BI Service & Admin</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>QlikView & Tableau</span>
                      <span className="text-sm text-muted-foreground">92%</span>
                    </div>
                    <Progress value={92} className="h-2" />
                  </div>
                  <div>
                    <div className="flex justify-between mb-2">
                      <span>Advanced Excel & Data Modeling</span>
                      <span className="text-sm text-muted-foreground">95%</span>
                    </div>
                    <Progress value={95} className="h-2" />
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
                      <span>GCP GCP BigQuery & AI Platform</span>
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
                    • Lead a team of 12 data scientists, analysts, and ML engineers across advanced analytics, AI/ML,
                    and business intelligence functions supporting TotalEnergies' energy transition strategy
                  </li>
                  <li>
                    • Developed and executed enterprise analytics strategy aligned with TotalEnergies' digital
                    transformation initiatives, delivering €120M+ in measurable business value across retail,
                    commercial, and trading operations
                  </li>
                  <li>
                    • Built best-in-class customer insights and personalization platform using ML/NLP for fuel retail
                    operations, increasing customer retention by 28% and cross-sell conversion by 35%
                  </li>
                  <li>
                    • Implemented real-time risk analytics and automated decision-making systems for commodity trading,
                    processing 5M+ transactions daily with 99.2% accuracy
                  </li>
                  <li>
                    • Established AI ethics framework and governance policies ensuring fairness, transparency, and POPIA
                    compliance across all ML models
                  </li>
                  <li>
                    • Championed data-driven culture through Power BI dashboards and self-service analytics, enabling
                    500+ business users across finance, operations, and commercial functions
                  </li>
                  <li>
                    • Led predictive analytics initiatives for energy demand forecasting and pricing optimization,
                    reducing forecast errors by 22% and improving margin optimization by 18%
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
                    for retail fuel stations and B2B energy clients
                  </li>
                  <li>
                    • Built customer segmentation and propensity models using Python/R, driving 45% improvement in
                    marketing campaign ROI and increasing customer lifetime value by 32%
                  </li>
                  <li>
                    • Developed customer churn prediction models identifying at-risk accounts, enabling proactive
                    retention strategies that reduced B2B customer attrition by 28%
                  </li>
                  <li>
                    • Implemented personalized pricing engine for commercial clients based on consumption patterns,
                    improving customer satisfaction scores by 40% while maintaining margins
                  </li>
                  <li>• Optimized Spark jobs reducing processing time by 70% and infrastructure costs by 45%</li>
                  <li>
                    • Implemented real-time streaming analytics processing 50M+ events daily with sub-second latency for
                    fuel station transactions and loyalty programs
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
                  <li>
                    • Led requirements gathering and stakeholder engagement sessions with C-level executives,
                    translating complex business needs into technical solutions and data strategies
                  </li>
                  <li>
                    • Conducted comprehensive process analysis and mapping, identifying bottlenecks and optimization
                    opportunities that improved operational efficiency by 35%
                  </li>
                  <li>• Built and maintained ETL pipelines processing 100GB+ daily using Apache Airflow and Python</li>
                  <li>
                    • Developed statistical models in Python/R for customer behavior analysis and churn prediction,
                    providing actionable insights that informed strategic business decisions
                  </li>
                  <li>
                    • Created executive dashboards in Tableau combining complex data signals into actionable insights,
                    facilitating data-driven decision-making across multiple business units
                  </li>
                  <li>
                    • Performed cost-benefit analysis and ROI modeling for proposed initiatives, ensuring alignment with
                    business objectives and optimal resource allocation
                  </li>
                  <li>
                    • Collaborated with data scientist to productionize ML models serving 1M+ predictions daily while
                    maintaining comprehensive documentation and user acceptance testing
                  </li>
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
          <div className="grid md:grid-cols-2 gap-6">
            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Insurance Analytics & Insights Platform</CardTitle>
                    <CardDescription className="text-base">
                      Real-time analytics for policy performance, claims trends, and customer behavior
                    </CardDescription>
                  </div>
                  <FileSpreadsheet className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Built an Insurance Analytics platform handling unstructured data from policy administration, claims,
                    and customer interactions. Enabled real-time analytics for operational and strategic decisions,
                    improving risk management and customer engagement.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">SQL Server</Badge>
                    <Badge variant="secondary">Python</Badge>
                    <Badge variant="secondary">Power BI</Badge>
                    <Badge variant="secondary">Scikit-learn</Badge>
                    <Badge variant="secondary">PyODBC</Badge>
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
                                      <li>• SQL Server for data modeling</li>
                                      <li>• Python for analytics, ML, and data processing</li>
                                      <li>• Power BI for interactive dashboards</li>
                                      <li>• Churn prediction models (Random Forest)</li>
                                      <li>• Customer segmentation (RFM analysis)</li>
                                      <li>• Claims root cause analysis</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Real-time insights into policy & claims performance</li>
                                      <li>• Proactive risk management</li>
                                      <li>• Personalized customer engagement</li>
                                      <li>• Reduced reporting time by 70%+</li>
                                      <li>• Improved marketing ROI by 45%</li>
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
                                      SQL - Insurance Analytics Models
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
                                      Python - Analytics & ML
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
                                      <FileText className="h-5 w-5" />
                                      Power BI - Analytics Dashboard
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

            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Customer Analytics Data Warehouse</CardTitle>
                    <CardDescription className="text-base">
                      Enterprise data warehouse for customer insights and marketing analytics
                    </CardDescription>
                  </div>
                  <BarChart3 className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Designed and implemented a cloud-native data warehouse consolidating data from 15+ sources to enable
                    360° customer view and advanced analytics with 200TB+ data.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">Snowflake</Badge>
                    <Badge variant="secondary">dbt</Badge>
                    <Badge variant="secondary">Airflow</Badge>
                    <Badge variant="secondary">Python</Badge>
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
                            {projectDetails["customer-warehouse"].title}
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
                                  {projectDetails["customer-warehouse"].problemStatement}
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
                                  src={projectDetails["customer-warehouse"].architecture || "/placeholder.svg"}
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
                                      <li>• Snowflake data warehouse</li>
                                      <li>• dbt for data transformations</li>
                                      <li>• Airflow for orchestration</li>
                                      <li>• Star schema data modeling</li>
                                      <li>• Incremental loading strategies</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• 200TB+ of customer data consolidated</li>
                                      <li>• Query performance improved by 10x</li>
                                      <li>• 360° customer view enabled</li>
                                      <li>• Self-service analytics for 500+ users</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                  <Database className="h-5 w-5" />
                                  SQL - Data Modeling with dbt
                                </CardTitle>
                              </CardHeader>
                              <CardContent>
                                <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                  <code>{projectDetails["customer-warehouse"].solution}</code>
                                </pre>
                              </CardContent>
                            </Card>
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
                                      <Database className="h-5 w-5" />
                                      MS Access - Data Management
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
                                      <FileText className="h-5 w-5" />
                                      PowerPoint VBA - Presentation Automation
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

            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Enterprise HR Data Platform</CardTitle>
                    <CardDescription className="text-base">
                      Unified HR data platform for workforce analytics, compliance, and executive reporting
                    </CardDescription>
                  </div>
                  <FileSpreadsheet className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Developed an enterprise HR data platform integrating data from 8 HRIS systems, enabling unified
                    workforce analytics and ensuring compliance. Reduced reporting delays from 2 weeks to real-time
                    dashboards through ETL pipelines, data governance, and Alteryx/Python automation.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">Azure SQL</Badge>
                    <Badge variant="secondary">Python</Badge>
                    <Badge variant="secondary">Alteryx</Badge>
                    <Badge variant="secondary">Power BI</Badge>
                    <Badge variant="secondary">Azure Blob Storage</Badge>
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
                                      <li>• Azure SQL DB for Silver & Gold layers</li>
                                      <li>• Azure Blob Storage for Bronze layer</li>
                                      <li>• Python for ETL orchestration & API integration</li>
                                      <li>• Alteryx for workflow automation & data prep</li>
                                      <li>• Power BI for workforce analytics dashboards</li>
                                      <li>• Data governance & PII masking</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Unified view of workforce analytics</li>
                                      <li>• Real-time reporting & compliance</li>
                                      <li>• Reduced reporting time by 80%+</li>
                                      <li>• Improved data quality and governance</li>
                                      <li>• Scalable and secure data infrastructure</li>
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
                                      SQL - HR Data Model
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
                                      Python - ETL Orchestration
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
                                      <FileText className="h-5 w-5" />
                                      Alteryx - Workflow Automation
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
                                      <FileText className="h-5 w-5" />
                                      Power BI - Workforce Analytics
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

            <Card className="hover:shadow-xl transition-all duration-300 hover:-translate-y-1">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div>
                    <CardTitle className="text-xl mb-2">Real-time Fraud Detection Pipeline</CardTitle>
                    <CardDescription className="text-base">
                      Scalable platform for ingesting and analyzing IoT sensor data from manufacturing equipment
                    </CardDescription>
                  </div>
                  <Cpu className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Built an IoT data processing pipeline handling 10M+ sensor events per minute with real-time anomaly
                    detection and predictive maintenance capabilities.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">Kafka</Badge>
                    <Badge variant="secondary">Spark</Badge>
                    <Badge variant="secondary">Scala</Badge>
                    <Badge variant="secondary">Cassandra</Badge>
                    <Badge variant="secondary">Time Series</Badge>
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
                            {projectDetails["iot-processing"].title}
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
                                  {projectDetails["iot-processing"].problemStatement}
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
                                  src={projectDetails["iot-processing"].architecture || "/placeholder.svg"}
                                  alt="IoT Processing Architecture"
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
                                      <li>• Kafka for event streaming</li>
                                      <li>• Spark Streaming in Scala</li>
                                      <li>• Cassandra for time-series storage</li>
                                      <li>• Real-time aggregations</li>
                                      <li>• Anomaly detection algorithms</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• 10M+ events processed per minute</li>
                                      <li>• Real-time anomaly detection</li>
                                      <li>• 40% reduction in equipment downtime</li>
                                      <li>• Predictive maintenance enabled</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                  <Code className="h-5 w-5" />
                                  Scala - Kafka Streams Processing
                                </CardTitle>
                              </CardHeader>
                              <CardContent>
                                <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                  <code>{projectDetails["iot-processing"].solution}</code>
                                </pre>
                              </CardContent>
                            </Card>
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
                    <CardTitle className="text-xl mb-2">Financial Risk Analytics Dashboard</CardTitle>
                    <CardDescription className="text-base">
                      Real-time risk monitoring and analytics platform for portfolio management
                    </CardDescription>
                  </div>
                  <TrendingUp className="h-8 w-8 text-accent flex-shrink-0" />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Developed a comprehensive risk analytics platform using R and statistical models to provide
                    real-time portfolio risk assessment and regulatory reporting.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="secondary">R</Badge>
                    <Badge variant="secondary">Shiny</Badge>
                    <Badge variant="secondary">PostgreSQL</Badge>
                    <Badge variant="secondary">Statistical Models</Badge>
                    <Badge variant="secondary">Monte Carlo</Badge>
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
                                      <li>• R for statistical analysis</li>
                                      <li>• Shiny for interactive dashboards</li>
                                      <li>• PostgreSQL for data storage</li>
                                      <li>• VaR and CVaR calculations</li>
                                      <li>• Monte Carlo simulations</li>
                                    </ul>
                                  </div>
                                  <div>
                                    <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                    <ul className="text-sm text-muted-foreground space-y-1">
                                      <li>• Real-time portfolio risk monitoring</li>
                                      <li>• Automated regulatory reporting</li>
                                      <li>• Improved risk assessment accuracy</li>
                                      <li>• Reduced reporting time by 80%</li>
                                    </ul>
                                  </div>
                                </div>
                              </CardContent>
                            </Card>
                          </TabsContent>
                          <TabsContent value="code" className="space-y-4">
                            <Card>
                              <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                  <Code className="h-5 w-5" />R - Risk Analytics & Monte Carlo
                                </CardTitle>
                              </CardHeader>
                              <CardContent>
                                <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm">
                                  <code>{projectDetails["risk-analytics"].solution}</code>
                                </pre>
                              </CardContent>
                            </Card>
                          </TabsContent>
                        </Tabs>
                      </DialogContent>
                    </Dialog>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* FCRM BI Platform */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <BarChart3 className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">Financial Crime</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Financial Crime Risk Management BI Platform</CardTitle>
                <CardDescription>
                  Unified AML, fraud detection, and SAR compliance reporting platform using SQL Server, SSIS, Power BI, and QlikView for PBB SA FCRM teams.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["SQL Server", "SSIS", "SSRS", "Power BI", "QlikView", "Python", "Oracle", "Excel"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground mb-4">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />85% faster reporting</span>
                  <span className="flex items-center gap-1"><Users className="h-4 w-4" />FCRM Team</span>
                </div>
                <div className="flex gap-2">
                  <Dialog>
                    <DialogTrigger asChild>
                      <Button variant="outline" size="sm"><ExternalLink className="h-4 w-4 mr-1" />View Details</Button>
                    </DialogTrigger>
                    <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
                      <DialogHeader>
                        <DialogTitle className="text-2xl font-serif">{projectDetails["fcrm-reporting"].title}</DialogTitle>
                        <DialogDescription>Financial Crime Risk Management BI Platform</DialogDescription>
                      </DialogHeader>
                      <Tabs defaultValue="problem" className="mt-4">
                        <TabsList className="grid w-full grid-cols-4">
                          <TabsTrigger value="problem">Problem</TabsTrigger>
                          <TabsTrigger value="architecture">Architecture</TabsTrigger>
                          <TabsTrigger value="solution">Solution</TabsTrigger>
                          <TabsTrigger value="code">Code</TabsTrigger>
                        </TabsList>
                        <TabsContent value="problem" className="mt-4">
                          <Card>
                            <CardHeader><CardTitle>Problem Statement</CardTitle></CardHeader>
                            <CardContent><p className="text-muted-foreground leading-relaxed">{projectDetails["fcrm-reporting"].problemStatement}</p></CardContent>
                          </Card>
                        </TabsContent>
                        <TabsContent value="architecture" className="mt-4">
                          <Card>
                            <CardHeader><CardTitle>System Architecture</CardTitle></CardHeader>
                            <CardContent>
                              <img src={projectDetails["fcrm-reporting"].architecture || "/placeholder.svg"} alt="FCRM BI Architecture" className="w-full rounded-lg" />
                            </CardContent>
                          </Card>
                        </TabsContent>
                        <TabsContent value="solution" className="mt-4">
                          <Card>
                            <CardHeader><CardTitle>Technical Solution</CardTitle></CardHeader>
                            <CardContent className="space-y-4">
                              <div className="grid md:grid-cols-2 gap-4">
                                <div>
                                  <h4 className="font-semibold mb-2">Key Components:</h4>
                                  <ul className="text-sm text-muted-foreground space-y-1">
                                    <li>- SQL Server stored procedures for risk scorecards</li>
                                    <li>- SSIS packages for daily ETL orchestration</li>
                                    <li>- SSRS for automated regulatory report distribution</li>
                                    <li>- Power BI dashboards for real-time risk monitoring</li>
                                    <li>- QlikView for associative AML/fraud exploration</li>
                                    <li>- Python for automation and SAS data integration</li>
                                  </ul>
                                </div>
                                <div>
                                  <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                  <ul className="text-sm text-muted-foreground space-y-1">
                                    <li>- Reduced reporting cycle from 5 days to 4 hours</li>
                                    <li>- 100% SAR filing compliance rate achieved</li>
                                    <li>- Unified view across AML, fraud, and SAR data</li>
                                    <li>- Automated daily risk scorecard distribution</li>
                                    <li>- 60% reduction in analyst data wrangling time</li>
                                  </ul>
                                </div>
                              </div>
                            </CardContent>
                          </Card>
                        </TabsContent>
                        <TabsContent value="code" className="space-y-4">
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><Database className="h-5 w-5" />SQL Server - FCRM Risk Scorecard</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-reporting"].solution === 'object' ? projectDetails["fcrm-reporting"].solution.sql : ''}</code></pre>
                            </CardContent>
                          </Card>
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><Code className="h-5 w-5" />Python - SSIS Trigger & Report Automation</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-reporting"].solution === 'object' ? projectDetails["fcrm-reporting"].solution.python : ''}</code></pre>
                            </CardContent>
                          </Card>
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><BarChart3 className="h-5 w-5" />Power BI DAX - Risk Dashboard Measures</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-reporting"].solution === 'object' ? projectDetails["fcrm-reporting"].solution.powerbi : ''}</code></pre>
                            </CardContent>
                          </Card>
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><FileSpreadsheet className="h-5 w-5" />QlikView - FCRM Data Model Script</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-reporting"].solution === 'object' ? projectDetails["fcrm-reporting"].solution.qlikview : ''}</code></pre>
                            </CardContent>
                          </Card>
                        </TabsContent>
                      </Tabs>
                    </DialogContent>
                  </Dialog>
                </div>
              </CardContent>
            </Card>

            {/* FCRM Data Automation */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <Cpu className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">Process Automation</Badge>
                </div>
                <CardTitle className="text-xl mb-2">FCRM Data Extraction & Process Automation Engine</CardTitle>
                <CardDescription>
                  Automated cross-platform data extraction from Oracle, SQL Server, and SAS datasets for AML investigations, regulatory compliance, and financial crime analytics.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["SQL Server", "Oracle", "Python", "SAS", "Excel", "QlikView", "SSIS"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground mb-4">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />15hrs/week saved</span>
                  <span className="flex items-center gap-1"><Users className="h-4 w-4" />FCRM Analysts</span>
                </div>
                <div className="flex gap-2">
                  <Dialog>
                    <DialogTrigger asChild>
                      <Button variant="outline" size="sm"><ExternalLink className="h-4 w-4 mr-1" />View Details</Button>
                    </DialogTrigger>
                    <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
                      <DialogHeader>
                        <DialogTitle className="text-2xl font-serif">{projectDetails["fcrm-data-automation"].title}</DialogTitle>
                        <DialogDescription>FCRM Data Extraction & Process Automation</DialogDescription>
                      </DialogHeader>
                      <Tabs defaultValue="problem" className="mt-4">
                        <TabsList className="grid w-full grid-cols-4">
                          <TabsTrigger value="problem">Problem</TabsTrigger>
                          <TabsTrigger value="architecture">Architecture</TabsTrigger>
                          <TabsTrigger value="solution">Solution</TabsTrigger>
                          <TabsTrigger value="code">Code</TabsTrigger>
                        </TabsList>
                        <TabsContent value="problem" className="mt-4">
                          <Card>
                            <CardHeader><CardTitle>Problem Statement</CardTitle></CardHeader>
                            <CardContent><p className="text-muted-foreground leading-relaxed">{projectDetails["fcrm-data-automation"].problemStatement}</p></CardContent>
                          </Card>
                        </TabsContent>
                        <TabsContent value="architecture" className="mt-4">
                          <Card>
                            <CardHeader><CardTitle>System Architecture</CardTitle></CardHeader>
                            <CardContent>
                              <img src={projectDetails["fcrm-data-automation"].architecture || "/placeholder.svg"} alt="FCRM Automation Architecture" className="w-full rounded-lg" />
                            </CardContent>
                          </Card>
                        </TabsContent>
                        <TabsContent value="solution" className="mt-4">
                          <Card>
                            <CardHeader><CardTitle>Technical Solution</CardTitle></CardHeader>
                            <CardContent className="space-y-4">
                              <div className="grid md:grid-cols-2 gap-4">
                                <div>
                                  <h4 className="font-semibold mb-2">Key Components:</h4>
                                  <ul className="text-sm text-muted-foreground space-y-1">
                                    <li>- Oracle linked server cross-platform queries</li>
                                    <li>- Python automation for SAS dataset conversion</li>
                                    <li>- Automated AML threshold monitoring</li>
                                    <li>- Structuring and smurfing pattern detection</li>
                                    <li>- Watchlist cross-referencing engine</li>
                                    <li>- QlikView associative data model</li>
                                  </ul>
                                </div>
                                <div>
                                  <h4 className="font-semibold mb-2">Results Achieved:</h4>
                                  <ul className="text-sm text-muted-foreground space-y-1">
                                    <li>- Saved 15+ hours per week in manual extractions</li>
                                    <li>- 100% FICA/FIC Act compliance on submissions</li>
                                    <li>- Automated sanctioned jurisdiction flagging</li>
                                    <li>- Real-time structuring pattern detection</li>
                                    <li>- Unified Oracle + SQL Server investigation view</li>
                                  </ul>
                                </div>
                              </div>
                            </CardContent>
                          </Card>
                        </TabsContent>
                        <TabsContent value="code" className="space-y-4">
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><Database className="h-5 w-5" />SQL - Oracle/SQL Server Cross-Platform Investigation</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-data-automation"].solution === 'object' ? projectDetails["fcrm-data-automation"].solution.sql : ''}</code></pre>
                            </CardContent>
                          </Card>
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><Code className="h-5 w-5" />Python - AML Automation & SAS Integration</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-data-automation"].solution === 'object' ? projectDetails["fcrm-data-automation"].solution.python : ''}</code></pre>
                            </CardContent>
                          </Card>
                          <Card>
                            <CardHeader><CardTitle className="flex items-center gap-2"><FileSpreadsheet className="h-5 w-5" />QlikView - FCRM Associative Data Model</CardTitle></CardHeader>
                            <CardContent>
                              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-sm"><code>{typeof projectDetails["fcrm-data-automation"].solution === 'object' ? projectDetails["fcrm-data-automation"].solution.qlikview : ''}</code></pre>
                            </CardContent>
                          </Card>
                        </TabsContent>
                      </Tabs>
                    </DialogContent>
                  </Dialog>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </section>

      {/* Power BI Portfolio Section */}
      <section id="powerbi" className="py-16 px-4 bg-card">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <Badge variant="secondary" className="mb-4">Power BI Expertise</Badge>
            <h2 className="text-4xl font-serif font-bold mb-4">Power BI Portfolio</h2>
            <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
              Showcasing my expertise in Microsoft Power BI - from interactive dashboards to enterprise-wide analytics solutions with DAX, Power Query, and data modeling.
            </p>
          </div>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {/* Power BI Project 1 - Executive Dashboard */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <BarChart3 className="h-5 w-5 text-primary" />
                  <Badge variant="outline" className="bg-amber-500/10 text-amber-600 border-amber-500/30">Featured</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Executive KPI Dashboard</CardTitle>
                <CardDescription>
                  Real-time executive dashboard providing C-suite visibility into sales performance, operational metrics, and financial KPIs across multiple business units.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["DAX", "Power Query", "Row-Level Security", "Paginated Reports", "Dataflows"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <h4 className="font-semibold text-sm mb-2">Key DAX Measures:</h4>
                  <pre className="text-xs overflow-x-auto"><code>{`// YTD Revenue with Time Intelligence
YTD Revenue = 
CALCULATE(
    SUM(Sales[Revenue]),
    DATESYTD('Calendar'[Date])
)

// Revenue vs Target Variance %
Variance % = 
DIVIDE(
    [Actual Revenue] - [Target Revenue],
    [Target Revenue],
    0
) * 100`}</code></pre>
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />40% faster decisions</span>
                  <span className="flex items-center gap-1"><Users className="h-4 w-4" />200+ users</span>
                </div>
              </CardContent>
            </Card>

            {/* Power BI Project 2 - Financial Analytics */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <TrendingUp className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">Finance</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Financial P&L Analytics</CardTitle>
                <CardDescription>
                  Comprehensive profit and loss analysis with drill-through capabilities, budget vs actual comparisons, and automated variance commentary.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["DAX", "Calculation Groups", "What-If Parameters", "Composite Models"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <h4 className="font-semibold text-sm mb-2">Advanced P&L DAX:</h4>
                  <pre className="text-xs overflow-x-auto"><code>{`// Dynamic P&L Line Calculation
P&L Value = 
VAR CurrentRow = 
    SELECTEDVALUE(PLStructure[LineItem])
RETURN
SWITCH(
    TRUE(),
    CurrentRow = "Revenue", [Total Revenue],
    CurrentRow = "COGS", [Cost of Goods Sold],
    CurrentRow = "Gross Profit", 
        [Total Revenue] - [Cost of Goods Sold],
    CurrentRow = "Net Income",
        [Gross Profit] - [Operating Expenses],
    BLANK()
)`}</code></pre>
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />Monthly close: 5 days to 1</span>
                </div>
              </CardContent>
            </Card>

            {/* Power BI Project 3 - HR Analytics */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <Users className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">HR Analytics</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Workforce Analytics Suite</CardTitle>
                <CardDescription>
                  Employee analytics covering headcount trends, attrition analysis, diversity metrics, and workforce planning with predictive modeling.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["DAX", "Power Query M", "DirectQuery", "Incremental Refresh"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <h4 className="font-semibold text-sm mb-2">Attrition Analysis DAX:</h4>
                  <pre className="text-xs overflow-x-auto"><code>{`// Rolling 12-Month Attrition Rate
Attrition Rate = 
VAR Terminations = 
    CALCULATE(
        COUNTROWS(Employees),
        Employees[Status] = "Terminated",
        DATESINPERIOD(
            'Calendar'[Date],
            MAX('Calendar'[Date]),
            -12, MONTH
        )
    )
VAR AvgHeadcount = 
    AVERAGEX(
        DATESINPERIOD(...),
        [Active Headcount]
    )
RETURN
DIVIDE(Terminations, AvgHeadcount, 0)`}</code></pre>
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />Reduced attrition 15%</span>
                </div>
              </CardContent>
            </Card>

            {/* Power BI Project 4 - Sales Pipeline */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <Target className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">Sales</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Sales Pipeline & CRM Analytics</CardTitle>
                <CardDescription>
                  End-to-end sales pipeline visualization with conversion funnels, win/loss analysis, and sales rep performance tracking integrated with Dynamics 365.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["DAX", "Dynamics 365 Connector", "Dataverse", "AI Insights"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <h4 className="font-semibold text-sm mb-2">Sales Funnel DAX:</h4>
                  <pre className="text-xs overflow-x-auto"><code>{`// Stage Conversion Rate
Stage Conversion = 
VAR CurrentStage = 
    SELECTEDVALUE(Pipeline[Stage])
VAR StageOrder = 
    SELECTEDVALUE(Pipeline[StageOrder])
VAR PrevStageCount = 
    CALCULATE(
        COUNTROWS(Opportunities),
        Pipeline[StageOrder] = StageOrder - 1
    )
VAR CurrentCount = 
    COUNTROWS(Opportunities)
RETURN
DIVIDE(CurrentCount, PrevStageCount, 0)`}</code></pre>
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />Win rate +22%</span>
                </div>
              </CardContent>
            </Card>

            {/* Power BI Project 5 - Supply Chain */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <Cpu className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">Operations</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Supply Chain & Inventory</CardTitle>
                <CardDescription>
                  Real-time inventory management dashboard with demand forecasting, stockout predictions, and supplier performance metrics.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["DAX", "R Visual", "Python Visual", "Streaming Dataset"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <h4 className="font-semibold text-sm mb-2">Inventory Health DAX:</h4>
                  <pre className="text-xs overflow-x-auto"><code>{`// Days of Inventory (DOI)
Days of Inventory = 
VAR AvgDailySales = 
    DIVIDE(
        [Total Units Sold],
        DISTINCTCOUNT('Calendar'[Date])
    )
VAR CurrentStock = 
    SUM(Inventory[OnHand])
RETURN
DIVIDE(CurrentStock, AvgDailySales, 0)

// Stockout Risk Flag
Stockout Risk = 
IF([Days of Inventory] < 7, "Critical",
IF([Days of Inventory] < 14, "Warning",
"Healthy"))`}</code></pre>
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />Stockouts -45%</span>
                </div>
              </CardContent>
            </Card>

            {/* Power BI Project 6 - Customer 360 */}
            <Card className="group hover:shadow-lg transition-all duration-300 border-border/50 hover:border-primary/30">
              <CardHeader>
                <div className="flex items-center gap-2 mb-2">
                  <Lightbulb className="h-5 w-5 text-primary" />
                  <Badge variant="secondary">Customer Analytics</Badge>
                </div>
                <CardTitle className="text-xl mb-2">Customer 360 Analytics</CardTitle>
                <CardDescription>
                  Unified customer view combining transactional data, behavioral insights, and predictive churn scoring with RLS for regional teams.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2 mb-4">
                  {["DAX", "Row-Level Security", "Bookmarks", "Drillthrough"].map((tech) => (
                    <Badge key={tech} variant="outline" className="text-xs">{tech}</Badge>
                  ))}
                </div>
                <div className="bg-muted rounded-lg p-4 mb-4">
                  <h4 className="font-semibold text-sm mb-2">Customer Lifetime Value DAX:</h4>
                  <pre className="text-xs overflow-x-auto"><code>{`// Customer Lifetime Value (CLV)
CLV = 
VAR AvgOrderValue = 
    AVERAGEX(
        Orders,
        Orders[OrderTotal]
    )
VAR PurchaseFrequency = 
    DIVIDE(
        COUNTROWS(Orders),
        DISTINCTCOUNT(Orders[CustomerID])
    )
VAR AvgLifespan = 3 // years
RETURN
AvgOrderValue * PurchaseFrequency * AvgLifespan`}</code></pre>
                </div>
                <div className="flex items-center gap-4 text-sm text-muted-foreground">
                  <span className="flex items-center gap-1"><TrendingUp className="h-4 w-4" />Retention +28%</span>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Power BI Certifications */}
          <div className="mt-12 text-center">
            <h3 className="text-xl font-semibold mb-4">Power BI Certifications & Skills</h3>
            <div className="flex flex-wrap justify-center gap-3">
              <Badge className="px-4 py-2 text-sm bg-primary/10 text-primary border-primary/30">Microsoft Certified: Power BI Data Analyst Associate</Badge>
              <Badge className="px-4 py-2 text-sm bg-primary/10 text-primary border-primary/30">DAX & Data Modeling Expert</Badge>
              <Badge className="px-4 py-2 text-sm bg-primary/10 text-primary border-primary/30">Power Query M Language</Badge>
              <Badge className="px-4 py-2 text-sm bg-primary/10 text-primary border-primary/30">Power BI Service Administration</Badge>
              <Badge className="px-4 py-2 text-sm bg-primary/10 text-primary border-primary/30">Paginated Reports (SSRS)</Badge>
              <Badge className="px-4 py-2 text-sm bg-primary/10 text-primary border-primary/30">Power BI Embedded</Badge>
            </div>
          </div>
        </div>
      </section>

      {/* Contact Section */}
      <section id="contact" className="py-16 px-4 bg-background">
        <div className="max-w-xl mx-auto text-center">
          <h2 className="text-4xl font-serif font-bold mb-4">Let's Connect</h2>
          <p className="text-lg text-muted-foreground mb-8">
            Have a project in mind or want to discuss how data can drive your business? Let's talk.
          </p>
          <Button size="lg" className="bg-primary hover:bg-primary/90" asChild>
            <a href="mailto:stanton.edwards@email.com">
              <Mail className="mr-2 h-4 w-4" />
              Say Hello
            </a>
          </Button>
        </div>
      </section>

      <footer className="py-8 px-4 text-center text-muted-foreground text-sm">
        © {new Date().getFullYear()} Stanton Edwards. All rights reserved.
      </footer>

      {/* Resume Modal */}
      <Dialog open={isResumeOpen} onOpenChange={setIsResumeOpen}>
        <DialogContent className="max-w-4xl w-full max-h-[90vh] overflow-hidden p-0">
          <DialogHeader className="p-6 pb-2">
            <DialogTitle className="text-2xl font-serif">Stanton Edwards - Curriculum Vitae</DialogTitle>
            <DialogDescription>
              Download a comprehensive PDF of my professional experience, skills, and achievements.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-6 py-6">
            <div className="text-center space-y-4">
              <div className="flex justify-center">
                <FileText className="h-16 w-16 text-primary" />
              </div>
              <h3 className="text-xl font-semibold">Professional CV</h3>
              <p className="text-muted-foreground max-w-2xl mx-auto">
                Complete curriculum vitae including 10+ years of experience in data analytics, AI/ML leadership,
                business intelligence, and advanced analytics across energy and financial services sectors.
              </p>
            </div>

            <div className="bg-muted/50 rounded-lg p-6 space-y-4">
              <h4 className="font-semibold text-lg">CV Includes:</h4>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="flex items-start gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5" />
                  <span className="text-sm">Comprehensive work experience (2016-Present)</span>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5" />
                  <span className="text-sm">Technical skills & certifications</span>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5" />
                  <span className="text-sm">Featured project highlights</span>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5" />
                  <span className="text-sm">Education & professional qualifications</span>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5" />
                  <span className="text-sm">Leadership & team management experience</span>
                </div>
                <div className="flex items-start gap-2">
                  <CheckCircle2 className="h-5 w-5 text-green-600 mt-0.5" />
                  <span className="text-sm">Contact information</span>
                </div>
              </div>
            </div>

            <div className="flex gap-3 justify-center">
              <Button onClick={downloadCV} size="lg" className="gap-2">
                <Download className="h-5 w-5" />
                Download CV as PDF
              </Button>
              <Button onClick={() => setIsResumeOpen(false)} variant="outline" size="lg">
                Close
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

export default Portfolio
