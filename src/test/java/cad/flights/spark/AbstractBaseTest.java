package cad.flights.spark;


import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractBaseTest<Input, Result> {

	protected SparkSession spark;
	
	
	protected void startSpark() {
		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		spark = SparkSession.builder().
				appName(this.getClass().getSimpleName()).
				master("local").
				getOrCreate();
		
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");
	}


	/**
	 * Pre-processing of the input file to generate a dataset of type Input
	 *
	 * @param file The input file
	 * @return The generated dataset
	 */
	protected abstract Input processInputFile(String file);

	/**
	 * The test to execute over the input dataset (of type Input)
	 *
	 * @param dataset The input dataset
	 * @return The result of the test
	 */
	protected abstract Result run(Input dataset);

	/**
	 * The result the test should produce.
	 * If the method returns null, the output of the test is not tested.
	 * @return The expected result of the test
	 */
	protected abstract String expectedResult();


	@Test
	public void run() {
		
		String file = "data/flights.csv";
		
		startSpark();

		Input flights = processInputFile(file);

		long start = System.currentTimeMillis();
		Result result = run(flights);
		long elapsed = System.currentTimeMillis() - start;
		
		System.err.println("Elapsed Time: " + elapsed);

		String expectedResult = expectedResult();
		if (expectedResult != null) {
			System.err.println("Asserting the correctness of the test's result");
			assertEquals(expectedResult(), result.toString());
		}
		// terminate the session
		spark.stop();
	}

}
