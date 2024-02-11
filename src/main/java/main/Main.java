package main;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {


    public static void main(String[] args) {

        // Create Spark session
        SparkSession sparkSession = SparkSession.builder().appName("IntegrationDonnees").master("local").getOrCreate();

        // Load Openfoodfacts data from CSV link
        Dataset<Row> openFoodFactsData = sparkSession.read()
                .format("csv")
                .option("header", "true")
                // Use tabulation as separator
                .option("delimiter", "\t")
                // Use UTF-8 encoding
                .option("encoding", "UTF-8")
                // CSV file path on my computer, change to work
                .load("C:/Users/lowkyz/Desktop/en.openfoodfacts.org.products.csv");


        // Initial information (root)
        openFoodFactsData.printSchema();

        // Row count
        long rowCount = openFoodFactsData.count();

        // Column count
        int columnCount = openFoodFactsData.columns().length;

        // Display result (this step provides an overview of the data volume)
        System.out.println("Le dataset compte " + rowCount + " lignes et " + columnCount + " variables.");

        // Create an array for the columns we want to keep
        String[] columnsToKeep = {"code", "product_name", "countries_en", "nutriscore_grade", "energy_100g", "fat_100g", "carbohydrates_100g", "proteins_100g", "salt_100g"};
        openFoodFactsData = openFoodFactsData.selectExpr(columnsToKeep);

        // First filter: We do not want the name, country, nutriscore or kcal number to be null
        openFoodFactsData = openFoodFactsData.filter("product_name is not null")
                .filter("countries_en is not null")
                .filter("nutriscore_grade is not null")
                .filter("energy_100g is not null")
                .filter("fat_100g is not null")
                .filter("carbohydrates_100g is not null")
                .filter("proteins_100g is not null")
                .filter("salt_100g is not null");

        // We want to remove values that are said to be "aberrant"
        // As the nutriscore is not a very important value, "unknown" is not bothersome
        // Convert relevant columns to double type for calculations
        for (String column : columnsToKeep) {
            // Check if the column is a string
            if (openFoodFactsData.schema().apply(column).dataType().simpleString().equals("string")) {
                // Leave the column unchanged
                continue;
            }
            // Convert the column to double
            openFoodFactsData = openFoodFactsData.withColumn(column, functions.col(column).cast("double"));
        }

        // Define acceptable ranges for each column
        // min limits
        double[] minValues = {1, 0, 0, 0, 0, 0};
        // max limits
        double[] maxValues = {900, 100, 100, 100, 100, 100};

        // Filter rows containing aberrant values (here for energy, fats, proteins, sugars, and salt)
        for (int i = 4; i < columnsToKeep.length; i++) {
            // Begin at "energy_100g"
            String column = columnsToKeep[i];
            double minValue = minValues[i - 4]; // Index relative to the bounds in the minValues array
            double maxValue = maxValues[i - 4]; // Index relative to the bounds in the maxValues array
            openFoodFactsData = openFoodFactsData.filter(openFoodFactsData.col(column).geq(minValue).and(openFoodFactsData.col(column).leq(maxValue)));
        }
        // Row count after cleaning
        long rowCountAfter = openFoodFactsData.count();

        // Column count after cleaning
        int columnCountAfter = openFoodFactsData.columns().length;

        // Display result (this step provides an overview of the data volume after cleaning)
        System.out.println("Après le netoyage, le dataset compte " + rowCountAfter + " lignes et " + columnCountAfter + " variables.");

        // Show the first 20 lines of the table
        openFoodFactsData.show();

        ////////////////////////////////////////////////////////////////////////

        // Load diet data from CSV file
        Dataset<Row> regimesData = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", ",")  // Using comma as separator
                .option("encoding", "UTF-8") // Using UTF-8 encoding
                .load("C:/Users/lowkyz/Desktop/regimes_nutritionnels.csv"); // CSV file path on my computer, change to work

        regimesData.show();

        // Load user diet data from CSV file
        Dataset<Row> utilisateursData = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", ",")  // Using comma as separator
                .option("encoding", "UTF-8") // Using UTF-8 encoding
                .load("C:/Users/lowkyz/Desktop/utilisateurs_regimes.csv"); // CSV file path on my computer, change to work

        utilisateursData.show();


        // Example code for exporting cleaned data if needed
        // Adjust the file path as necessary for your environment
//        String outputPath = "C:/Users/lowkyz/Desktop/cleaned_openfoodfacts_data.csv";
//        openFoodFactsData.write()
//                .format("csv")
//                .option("header", "true")
//                .mode("overwrite")
//                .save(outputPath);
//
//        System.out.println("Data export completed successfully.");

    }

}
