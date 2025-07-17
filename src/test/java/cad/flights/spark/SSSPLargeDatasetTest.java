package cad.flights.spark;

import cad.flights.DatasetGenerator;
import cad.flights.Flight;
import cad.flights.Route;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;


public class SSSPLargeDatasetTest extends AbstractFlightsTest<Route> {

    @Override
    protected JavaRDD<Flight> processInputFile(String file) {
        DatasetGenerator generator = new DatasetGenerator(2000, 5, 12345);
        return generator.build(spark.sparkContext());
    }

    @Override
    protected Route run(JavaRDD<Flight> flights) {
        long startTime = System.currentTimeMillis();
        Route route = new SSSP("Node0", "Node950", flights).run();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double weight = route.getWeight();
        System.out.println("Route " + route + " with weight " + weight + ", computed in " + duration + " ms (parallel)");

        Assertions.assertNotNull(route, "The route must not be null.");

        Assertions.assertTrue(weight > 0 && weight < Double.POSITIVE_INFINITY,
                "Weight must be strictly positive and not infinite.");

        String path = route.toString();
        Assertions.assertTrue(path.startsWith("Node0"),
                "The route should start by Node0");
        Assertions.assertTrue(path.endsWith("Node950"),
                "The route should end by Node950");

        return route;
    }

    @Override
    protected String expectedResult() {
        // everything in run()
        return null;
    }
}
