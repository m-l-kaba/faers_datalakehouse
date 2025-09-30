# FAERS Data Lakehouse

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.0.1-orange.svg)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-red.svg)](https://databricks.com/)
[![Terraform](https://img.shields.io/badge/Terraform-Infrastructure-purple.svg)](https://terraform.io/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-green.svg)](https://delta.io/)

A production-ready, end-to-end data lakehouse solution for FDA Adverse Event Reporting System (FAERS) data, built with modern data engineering best practices. This project demonstrates advanced data engineering skills including cloud infrastructure automation, data pipeline orchestration, and dimensional modeling.

## Architecture Overview

This lakehouse implements the **medallion architecture** (Bronze → Silver → Gold) with Unity Catalog governance and enterprise-grade infrastructure.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    LANDING      │───▶│     BRONZE      │───▶│     SILVER      │
│                 │    │                 │    │                 │
│ • Raw FAERS     │    │ • Schema        │    │ • Cleaned       │
│   data files    │    │   validation    │    │ • Standardized  │
│ • Volume-based  │    │ • Partitioned   │    │ • Typed         │
│   ingestion     │    │ • Timestamped   │    │ • Validated     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │    ANALYTICS    │◀───│      GOLD       │
                       │                 │    │                 │
                       │ • Insights      │    │ • Star schema   │
                       │ • Aggregations  │    │ • Fact tables   │
                       │ • Reports       │    │ • Dimensions    │
                       │ • Dashboards    │    │ • SCD Type 2    │
                       └─────────────────┘    └─────────────────┘
```

## Project Objectives

This portfolio project demonstrates expertise in:

- **Cloud Data Engineering**: Azure + Databricks lakehouse architecture
- **Infrastructure as Code**: Terraform modules for reproducible deployments
- **Data Pipeline Orchestration**: Complex DAG workflows with dependencies
- **Data Modeling**: Dimensional modeling with slowly changing dimensions
- **Data Governance**: Unity Catalog with proper permissions and lineage
- **Software Engineering**: Testing, logging, configuration management
- **DevOps Practices**: CI/CD ready with Databricks Asset Bundles

## Technology Stack

### **Cloud Infrastructure**

- **Azure**: Resource management and storage
- **Databricks**: Unified analytics platform
- **Unity Catalog**: Data governance and security
- **Delta Lake**: ACID transactions and time travel

### **Data Processing**

- **PySpark**: Distributed data processing
- **Python 3.13**: Modern Python with type hints
- **Delta Tables**: Optimized storage format

### **Infrastructure & DevOps**

- **Terraform**: Infrastructure as Code with reusable modules
- **Databricks Asset Bundles**: Environment management
- **Git**: Version control and collaboration

### **Development Tools**

- **pytest**: Comprehensive testing framework
- **ruff**: Fast Python linting and formatting
- **uv**: Fast Python package management
- **VS Code**: Development environment

## Data Sources

The project processes **FDA Adverse Event Reporting System (FAERS)** data, which includes:

- **Demographics**: Patient information and characteristics
- **Drug Details**: Medication information and classifications
- **Reactions**: Adverse event descriptions and outcomes
- **Indications**: Medical conditions being treated
- **Outcomes**: Patient outcomes and severity
- **Reports**: Reporting source and metadata
- **Therapy Dates**: Treatment timelines and durations

## Project Structure

```
faers_datalakehouse/
├── infra/terraform/              # Infrastructure as Code
│   ├── modules/faers-lakehouse/  # Reusable Terraform module
│   ├── main.tf                   # Module usage example
│   └── providers.tf              # Provider configurations
│
├── src/                          # Data pipeline source code
│   ├── bronze/                   # Raw data ingestion (7 files)
│   ├── silver/                   # Data cleaning & standardization (7 files)
│   ├── gold/                     # Dimensional modeling
│   │   ├── dims/                 # Dimension tables (8 files)
│   │   ├── facts/                # Fact tables
│   │   └── analytics/            # Business insights
│   └── utils/                    # Shared utilities & configurations
│
├── resources/                    # Databricks workflows
│   ├── jobs/                     # Pipeline definitions
│   │   ├── faers_pipeline.yml    # Complete end-to-end pipeline
│   │   ├── ingest_bronze.yml     # Bronze layer jobs
│   │   ├── transform_silver.yml  # Silver layer jobs
│   │   ├── gold_dimensions.yml   # Dimension processing
│   │   └── gold_facts.yml        # Fact table processing
│   ├── clusters/                 # Cluster configurations
│   └── workflows/                # Workflow definitions
│
├── tests/                        # Comprehensive test suite
│   └── unit/                     # Unit tests for transformations
│
├── databricks.yml               # Databricks Asset Bundle config
├── pyproject.toml              # Python project configuration
└── README.md                   # This file
```

## Key Features

### **1. Infrastructure as Code**

- **Reusable Terraform Module**: Complete Azure + Databricks setup
- **Environment Management**: Dev/Staging/Prod configurations
- **Security Best Practices**: Unity Catalog permissions and RBAC
- **Cost Optimization**: Configurable cluster sizes and auto-termination

### **2. Data Pipeline Architecture**

- **Medallion Architecture**: Bronze → Silver → Gold data flow
- **Dependency Management**: Proper task orchestration with DAGs
- **Error Handling**: Comprehensive logging and monitoring
- **Performance Optimization**: Delta Lake optimizations and partitioning

### **3. Data Quality & Governance**

- **Schema Validation**: Enforced schemas at each layer
- **Data Lineage**: Full traceability through Unity Catalog
- **Quality Checks**: Data validation and profiling
- **Audit Trail**: Complete ingestion and transformation metadata

### **4. Advanced Data Modeling**

- **Star Schema Design**: Optimized for analytics workloads
- **Slowly Changing Dimensions**: SCD Type 2 implementation
- **Fact Table Design**: Comprehensive adverse event analytics
- **Business Logic**: Domain-specific transformations

### **5. Software Engineering Best Practices**

- **Modular Design**: Reusable components and utilities
- **Configuration Management**: Environment-specific configs
- **Testing Strategy**: Unit tests for all transformations
- **Code Quality**: Type hints, linting, and formatting

## Prerequisites

- **Azure Subscription** with Databricks workspace
- **Terraform** >= 1.0
- **Python** >= 3.13
- **Databricks CLI** configured
- **Git** for version control

## Quick Start

### 1. **Infrastructure Deployment**

```bash
# Navigate to infrastructure directory
cd infra/terraform

# Set up environment variables
source .env

# Initialize and deploy infrastructure
terraform init
terraform plan
terraform apply
```

### 2. **Data Pipeline Deployment**

```bash
# Install dependencies
uv install

# Deploy Databricks assets
databricks bundle deploy --target development

# Run the complete pipeline
databricks bundle run faers_pipeline --target development
```

### 3. **Verify Deployment**

```bash
# Run tests
uv run pytest tests/

# Check data quality
databricks bundle run gold_facts --target development
```

## Configuration

### **Environment Variables**

Set up your `.env` file with Azure and Databricks credentials:

```bash
# Azure Configuration
export ARM_CLIENT_ID="your-client-id"
export ARM_CLIENT_SECRET="your-client-secret"
export ARM_SUBSCRIPTION_ID="your-subscription-id"
export ARM_TENANT_ID="your-tenant-id"

# Databricks Configuration
export TF_VAR_account_id="your-databricks-account-id"
export TF_VAR_metastore_id="your-unity-catalog-metastore-id"
```

### **Terraform Module Configuration**

The infrastructure is highly configurable:

```hcl
module "faers_lakehouse" {
  source = "./modules/faers-lakehouse"

  # Environment Configuration
  environment = "production"
  location   = "East US"

  # Storage Configuration
  storage_account_tier = "Premium"
  storage_replication_type = "GRS"

  # Databricks Configuration
  databricks_sku = "premium"

  # Catalog Configuration
  catalogs = {
    production = {
      comment = "Production FAERS data"
    }
    development = {
      comment = "Development FAERS data"
    }
  }
}
```

## Data Pipeline Details

### **Bronze Layer** (Raw Data Ingestion)

- **Purpose**: Ingest raw FAERS data files with minimal transformation
- **Format**: Delta tables with ingestion metadata
- **Schema**: Flexible schema with data validation
- **Partitioning**: By ingestion date for performance

### **Silver Layer** (Data Cleaning & Standardization)

- **Purpose**: Clean, validate, and standardize data
- **Transformations**:
  - Date standardization and validation
  - Numeric field conversion and validation
  - Text cleaning and normalization
  - Business rule application
- **Quality Checks**: Comprehensive data validation
- **Performance**: Optimized for downstream consumption

### **Gold Layer** (Dimensional Modeling)

- **Star Schema Design**: Optimized for analytics
- **Dimension Tables**: 8 dimension tables with SCD Type 2
- **Fact Tables**: Comprehensive adverse event analytics
- **Business Logic**: Domain-specific calculations and KPIs

## Key Workflows

### **1. Complete Pipeline** (`faers_pipeline.yml`)

End-to-end data processing with proper dependencies:

- Bronze ingestion (7 parallel tasks)
- Silver transformations (7 parallel tasks)
- Gold dimension processing (8 sequential tasks)
- Fact table creation (1 final task)

### **2. Individual Layer Processing**

- **Bronze**: `ingest_bronze.yml` - Raw data ingestion
- **Silver**: `transform_silver.yml` - Data cleaning
- **Gold Dimensions**: `gold_dimensions.yml` - Dimension processing
- **Gold Facts**: `gold_facts.yml` - Fact table creation

## Testing Strategy

```bash
# Run all tests
uv run pytest tests/

# Run specific test categories
uv run pytest tests/unit/test_silver_transformations.py
uv run pytest tests/unit/test_scd_type2.py
uv run pytest tests/unit/test_data_loader.py
```

## CI/CD Process

This project implements a comprehensive CI/CD pipeline using GitHub Actions and Databricks Asset Bundles, enabling automated testing, deployment, and monitoring across multiple environments.

### **Pipeline Architecture**

```
┌─────────────────┐                           ┌─────────────────┐
│   DEVELOPMENT   │──────── Tag Created ────▶│   PRODUCTION    │
│                 │                           │                 │
│ • PR Merged     │                           │ • Tagged        │
│ • Auto Deploy  │                           │   Release       │
│ • Integration   │                           │ • Auto Deploy  │
│ • Testing       │                           │ • Monitoring    │
└─────────────────┘                           └─────────────────┘
        ▲
        │
        │ PR Merged
        │
┌─────────────────┐
│ PULL REQUEST    │
│                 │
│ • Code Checks   │
│ • Unit Tests    │
│ • Validation    │
│ • Review        │
└─────────────────┘
```

### **CI/CD Workflow**

#### **1. Pull Request Stage**

When a pull request is created, the following automated checks run:

- **Code Quality Checks**: Automated linting and formatting validation using ruff
- **Type Checking**: Static type analysis to catch type-related errors
- **Unit Testing**: Comprehensive test suite execution with coverage reporting
- **Infrastructure Validation**: Terraform syntax and configuration validation
- **Databricks Bundle Validation**: YAML configuration and dependency validation
- **Security Scanning**: Automated vulnerability detection in dependencies

#### **2. Development Deployment**

When a pull request is merged to main branch:

- **Automatic Trigger**: Deployment starts immediately after merge
- **Infrastructure Update**: Updates development infrastructure if needed
- **Asset Deployment**: Deploys Databricks workflows, jobs, and configurations to development environment
- **Integration Testing**: Runs end-to-end pipeline tests with sample data
- **Validation**: Ensures all components work together correctly

#### **3. Production Deployment**

When a tag is created on the main branch:

- **Release Trigger**: Tag creation automatically triggers production deployment
- **Infrastructure Provisioning**: Updates production infrastructure using Terraform
- **Asset Deployment**: Deploys Databricks assets to production environment
- **Health Checks**: Automated validation of system health post-deployment
- **Monitoring Activation**: Ensures all monitoring and alerting systems are active

### **Environment Management**

#### **Development Environment**

- **Purpose**: Feature development, testing, and validation
- **Deployment Trigger**: Automatic deployment when PR is merged to main branch
- **Data**: Sample datasets and synthetic data for testing
- **Compute**: Cost-optimized single-node clusters with auto-termination
- **Permissions**: Developer access with full control for experimentation
- **Testing**: Integration testing and end-to-end pipeline validation

## Business Value

This lakehouse enables:

- **Regulatory Compliance**: FDA reporting and analysis
- **Drug Safety Monitoring**: Real-time adverse event tracking
- **Research Insights**: Population health analytics
- **Risk Assessment**: Predictive modeling capabilities
- **Operational Efficiency**: Automated data processing

## Future Enhancements

- **Real-time Streaming**: Kafka/Event Hubs integration
- **ML Pipeline**: MLflow model training and serving
- **Data Catalog**: Enhanced metadata management
- **Alerting System**: Automated data quality monitoring
- **API Layer**: RESTful data access services

## Contributing

This is a portfolio project demonstrating data engineering best practices. The codebase follows:

- **PEP 8**: Python style guidelines
- **Type Hints**: Full type annotation
- **Documentation**: Comprehensive docstrings
- **Testing**: High test coverage
- **Git Flow**: Feature branch workflow

## Contact

**Mory Leon Kaba**  
Data Engineer | Cloud Solutions Architect

- Email: m.leon.kaba@gmail.com
- LinkedIn: [linkedin.com/in/mory-leon-kaba](https://www.linkedin.com/in/mory-kaba-80b5641a0)

---

_This project showcases advanced data engineering skills with production-ready code, comprehensive testing, and enterprise-grade architecture. It demonstrates proficiency in cloud data platforms, infrastructure automation, and modern data engineering practices._
