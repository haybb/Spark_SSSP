package cad.flights.seq;

import cad.flights.DatasetGenerator;
import cad.flights.Flight;
import cad.flights.Route;
import org.junit.jupiter.api.Assertions;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;


public class SSSPLargeDatasetSeqTest extends AbstractFlightsTest<Route> {

    @Override
    protected List<Flight> processInputFile(String file) {
        DatasetGenerator generator = new DatasetGenerator(2000, 5, 12345);
        SparkConf conf = new SparkConf()
                .setAppName("SeqLargeTest")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(new SparkContext(conf));
        List<Flight> list = generator.build(sc.sc()).collect();
        sc.stop();
        return list;
    }

    @Override
    protected Route run(List<Flight> flights) {
        long startTime = System.currentTimeMillis();
        Route route = new cad.flights.seq.SSSP("Node0", "Node950", flights).run();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double weight = route.getWeight();
        System.out.println("Route " + route + " with weight " + weight + ", computed in " + duration + " ms (sequential)");

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
