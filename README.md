# Apple Data Analysis Using Apache Spark (PySpark)

## ETL Workflows for Customer Transactions

This repository contains two ETL workflows developed with PySpark to analyze customer transactions, identifying:
1. Customers who bought AirPods after purchasing an iPhone.
2. Customers who bought only iPhones.

## Workflows Overview

### FirstWorkflow
Identifies customers who bought AirPods following an iPhone purchase.
- **Extraction**: Data from CSV and Delta table.
- **Transformation**: Finds customers with AirPods post-iPhone purchase.
- **Loading**: Saves transformed data to the output sink.

### SecondWorkflow
Identifies customers with only iPhone purchases.
- **Extraction**: Reuses data from `FirstWorkflow`.
- **Transformation**: Filters to find only-iPhone customers.
- **Loading**: Saves filtered data to the output sink.

## Usage

To run a workflow:
```python
workflow_name = "SecondWorkflow"
workflow_runner = WorkFlowRunner(workflow_name).runner()
```

## Prerequisites
- Apache Spark, PySpark, Databricks (or Spark-compatible environment)
