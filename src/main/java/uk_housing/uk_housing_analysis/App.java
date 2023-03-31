package uk_housing.uk_housing_analysis;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;



public class App {

	public static void main(String[] args) {

		App sql = new App();
		
		
		//Processing UK Housing Analysis
		//1st argument is input datafile & 2nd argument is output file directory.
		sql.housingAnalysis("../../sampleuk.csv", "generated");

	}
	private void housingAnalysis(String fileName, String outputPath) {

		//initiating spark session to process the data
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark Sql Example").master("local[2]")
				.getOrCreate();
		//creating the schema as per the csv file
		StructType struct = new StructType().add("TransactionId", "string").add("Price", "int").add("DateofTransfer", "string")
				.add("PropertyType", "string").add("PropertyStatus", "string").add("Duration", "string").add("City", "string")
				.add("District", "string").add("County", "string").add("PPDCategoryType", "string").add("RecordStatus", "string");
				

		//creating the dataframe
		//location of dataset s3 URL
		Dataset<Row> df = sparkSession.read().schema(struct).option("header", true).option("delimiter", ",")
				.csv("sampleuk.csv" + fileName);
		//creating the in memory table as reviews
		df.createOrReplaceTempView("analysis");
		
		//Extracting data in dataset for price analysis of Berkshire county.
		Dataset<Row> result = sparkSession
				.sql("SELECT Price,Duration,District,PropertyType,PropertyStatus FROM analysis where County='BERKSHIRE'");
		//to view data result on terminal
		result.show();
		//to save the data result in specific location with csv file type.
		result.write().option("header", true).csv(outputPath + "/berkshireAnalysis");
		
		//Extracting data in dataset for District wise Price Analysis.
		Dataset<Row> district = sparkSession
				.sql("SELECT Price,District FROM analysis ORDER BY Price DESC");
		//to view data result on terminal
		district.show();
		//to save the data result in specific location with csv file type
		district.write().option("header", true).csv(outputPath + "/districtAnalysis");
		
		//Extracting data in dataset for analysing duration count as 'F' specific for a district.
		Dataset<Row> count = sparkSession
				.sql("SELECT District, COUNT(Duration) as count FROM analysis where Duration='F' GROUP BY District ORDER BY count DESC");
		//to view data result on terminal
		count.show();
		//to save the data result in specific location with csv file type
		count.write().option("header", true).csv(outputPath + "/countDuration");
		
		//Extracting data in dataset for analysing ProprtyType count as 'S' for 10 cities.
		Dataset<Row> countCities = sparkSession
				.sql("SELECT City,COUNT(PropertyType) AS total_count FROM analysis where PropertyType='S' GROUP BY City ORDER BY total_count DESC limit 10");
		//to view data result on terminal
		countCities.show();
		//to save the data result in specific location with csv file type
		countCities.write().option("header", true).csv(outputPath + "/countCities");
		
		// Extracting data in dataset for analysisng city wise average price.
		Dataset<Row> avgPrice = sparkSession
				.sql("SELECT City,AVG(Price) AS total_price FROM analysis GROUP BY City ORDER BY total_price");
		//to view data result on terminal
		avgPrice.show();
		//to save the data result in specific location with csv file type
		avgPrice.write().option("header", true).csv(outputPath + "/avgPriceCity");
		
		//Extracting data in dataset for analysing TransactionId, PropertyType, County for specific PropertyStatus.
		Dataset<Row> propertyStatus  = sparkSession
				.sql("SELECT TransactionId,PropertyType,County FROM analysis where PropertyStatus='N'");
		//to view data result on terminal
		propertyStatus.show();
		//to save the data result in specific location with csv file type
		propertyStatus.write().option("header", true).csv(outputPath + "/propertyStatusAnalysis");
		
		//Extracting data in dataset analysing District wise average price.
		Dataset<Row> districtPrice = sparkSession
				.sql("SELECT District,AVG(Price) AS total_price FROM analysis GROUP BY District ORDER BY total_price DESC");
		//to view data result on terminal
		districtPrice.show();
		//to save the data result in specific location with csv file type
		districtPrice.write().option("header", true).csv(outputPath + "/districtPriceAnalysis");
		
		//Extracting data in dataset for analysing County wise average price.
		Dataset<Row> countyPrice = sparkSession
				.sql("SELECT County,AVG(Price) AS total_price FROM analysis GROUP BY County ORDER BY total_price DESC");
		//to view data result on terminal
		countyPrice.show();
		//to save the data result in specific location with csv file type
		countyPrice.write().option("header", true).csv(outputPath + "/countyPriceAnalysis");
		
		//Extracting data in dataset for City wise avg price analysis of property status as New
		Dataset<Row> newProperty = sparkSession
				.sql("SELECT City,AVG(Price) AS total_price FROM analysis where PropertyStatus='N' GROUP BY City");
		//to view data result on terminal
		newProperty.show();
		//to save the data result in specific location with csv file type
		newProperty.write().option("header", true).csv(outputPath + "/newPropertyAnalysis");
		
		//Extracting data in dataset for City wise avg price analysis of property status as Old
		Dataset<Row> oldProperty = sparkSession
				.sql("SELECT City,AVG(Price) AS total_price FROM analysis where PropertyStatus='Y' GROUP BY City");
		//to view data result on terminal
		oldProperty.show();
		//to save the data result in specific location with csv file type
		oldProperty.write().option("header", true).csv(outputPath + "/oldPropertyAnalysis");
		
		//Extracting data in dataset for District wise avg price analysis of Duration='L'.
		Dataset<Row> durationPrice = sparkSession
				.sql("SELECT District,AVG(Price) AS total_price FROM analysis where Duration='L' GROUP BY District");
		//to view data result on terminal
		durationPrice.show();
		//to save the data result in specific location with csv file type
		durationPrice.write().option("header", true).csv(outputPath + "/durationPriceAnalysis");
		
		//Extracting data in dataset for analysing avg price of top 5 cities.
		Dataset<Row> topCities = sparkSession
				.sql("SELECT City, AVG(Price) as average_price FROM analysis GROUP BY City ORDER BY average_price DESC LIMIT 5");
		//to view data result on terminal
		topCities.show();
		//to save the data result in specific location with csv file type
		topCities.write().option("header", true).csv(outputPath + "/topCitiesAnalysis");
		
	}
	
	
}

