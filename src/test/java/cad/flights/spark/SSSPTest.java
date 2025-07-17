package cad.flights.spark;

import cad.flights.Flight;
import cad.flights.Route;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SSSPTest extends AbstractFlightsTest<Route> {

	@Override
	protected Route run(JavaRDD<Flight> flights) {

		long time = System.currentTimeMillis();
		Route route = new SSSP("TPA", "PSG", flights).run();
		long elapsed =  System.currentTimeMillis() - time;
		System.out.println("Route " + route + " with weight " + route.getWeight() + "\nComputed in " + elapsed + " ms.");
		return route;
	}

	/**
	 * Copied this function from cad.flights.seq.AbstractFlightsTest to make it easier to get the List<Flight>
	 * in testGraph()
	 */
	protected List<Flight>  processInputFileSeq(String file) {
		try (Stream<String> lines = Files.lines(new File(file).toPath())) {
			return lines.map(Flight::parseFlight).collect(Collectors.toList());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	/**
	 * Test that the graph of the parallel version is equivalent to the graph of the sequential version
	 * I.e they are of the same size and that they contain the same value for the same indices
	 */
	public void testGraph() {
		String file = "data/flights.csv";
		startSpark();
		JavaRDD<Flight> flightsSpark = processInputFile(file);
		List<Flight> flightsSeq = processInputFileSeq(file);
		SSSP ssspSpark = new SSSP("TPA", "PSG", flightsSpark);
		JavaPairRDD<String,Tuple2<String,Double>> graphSpark = ssspSpark.getGraph();
		cad.flights.seq.SSSP ssspSeq = new cad.flights.seq.SSSP("TPA", "PSG", flightsSeq);
		List<MatrixEntry> graphSeq = ssspSeq.getGraph();

		Map<Tuple2<Long,Long>,Double> lookup = new HashMap<>();
		for (MatrixEntry matrixEntry : graphSeq) {
			lookup.put(new Tuple2<>(matrixEntry.i(), matrixEntry.j()), matrixEntry.value());
		}

		List<Tuple2<String, Tuple2<String, Double>>> graphSparkList = graphSpark.collect();
		assertEquals(graphSeq.size(), graphSparkList.size(),"The size of the two graphs should be the same");
		for (Tuple2<String, Tuple2<String, Double>> entrySpark : graphSparkList) {
			long origId = Flight.getAirportIdFromName(entrySpark._1);
			long destId = Flight.getAirportIdFromName(entrySpark._2._1);
			Tuple2<Long,Long> key = new Tuple2<>(origId, destId);
			assertTrue(lookup.containsKey(key), "This key corresponding of a flight is not in the sequential version : " + key);
			assertEquals(entrySpark._2._2, lookup.get(key), 1e-6);
		}

		spark.stop();
	}

	@Override
	protected String expectedResult() {
		return "TPA -> MCI -> SEA -> KTN -> WRG -> PSG";
	}
}
