# OpenFoodFacts Data Processing with Apache Spark - HADDANE Hassan & SADI Leina L1C3 (EPSI)

## Overview

This project is an implementation of an ETL (Extract, Transform, Load) process using Apache Spark in Java. It focuses on the extraction, cleaning, and preparation of nutritional data from OpenFoodFacts for further analysis and application, such as personalized diet planning.

## Features

- **Data Extraction**: Loads nutritional data from OpenFoodFacts and additional datasets related to dietary regimes and user profiles.
- **Data Cleaning**: Implements data cleaning techniques to remove null values and filter out aberrant nutritional values based on predefined criteria.
- **Data Transformation**: Selects relevant columns and converts data types for accurate analysis.
- **Custom Filtering**: Applies custom filters to ensure data quality and relevance for dietary analysis.
- **Data Preparation**: Prepares cleaned and transformed data for further analysis or export.

## Requirements

- Java Development Kit (JDK)
- Apache Spark
- CSV files containing OpenFoodFacts data, dietary regimes, and user profiles

## Setup and Execution

1. **Installation**: Ensure that Java and Apache Spark are installed on your system.
2. **Dataset**: Place your OpenFoodFacts dataset and other CSV files in an accessible directory.
3. **Configuration**: Update file paths in the code to match your dataset locations.
4. **Execution**: Compile and run the Java application using your preferred IDE or command line tools.

## Usage

```java
SparkSession sparkSession = SparkSession.builder().appName("IntegrationDonnees").master("local").getOrCreate();
// Load and process OpenFoodFacts data
Dataset<Row> openFoodFactsData = sparkSession.read().format("csv").option("header", "true").load("path_to_your_dataset");
// Follow the provided code structure for data processing
```

## Contribution

Contributions are welcome! Please fork the repository and submit a pull request with your proposed changes or enhancements.

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgments

- OpenFoodFacts for providing the nutritional data.
- Apache Spark for the powerful data processing framework.

---

This README provides a concise overview of the project setup, features, and guidelines for contributing. Ensure all necessary tools and datasets are available and correctly configured before proceeding with the execution of the project.
