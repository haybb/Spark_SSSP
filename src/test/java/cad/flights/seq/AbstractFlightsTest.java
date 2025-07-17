package cad.flights.seq;

import cad.flights.Flight;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;


public abstract class AbstractFlightsTest<Result>  {

	protected List<Flight>  processInputFile(String file) {
		try (Stream<String> lines = Files.lines(new File(file).toPath())) {
			return lines.map(Flight::parseFlight).collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * The test to execute over the input dataset (of type Input)
	 *
	 * @param dataset The input dataset
	 * @return The result of the test
	 */
	protected abstract Result run(List<Flight> dataset);

	/**
	 * The result the test should produce.
	 * If the method returns null, the output of the test is not tested.
	 * @return The expected result of the test
	 */
	protected abstract String expectedResult();

	@Test
	public void run() {

		String file = "data/flights.csv";

		List<Flight> flights = processInputFile(file);

		long start = System.currentTimeMillis();
		Result result = run(flights);
		long elapsed = System.currentTimeMillis() - start;

		System.err.println("Elapsed Time: " + elapsed);

		String expectedResult = expectedResult();
		if (expectedResult != null) {
			System.err.println("Asserting the correctness of the test's result");
			assertEquals(expectedResult(), result.toString());
		}
	}

}
