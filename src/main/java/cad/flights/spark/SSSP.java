package cad.flights.spark;

import cad.flights.AbstractFlightAnalyser;
import cad.flights.Flight;

import cad.flights.Route;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A Spark implementation of Dijkstra Single Source Shortest Path
 */
public class SSSP extends AbstractFlightAnalyser<JavaRDD<Flight>, Route> {

    /**
     * Representation of absence of edge between two nodes
     */
    private static final double NO_EDGE = Double.MAX_VALUE;

    /**
     * Representation of absence of predecessor for a given node in the current path
     */
    private static final String NO_PREDECESSOR = "";

    /**
     * The name of the source node (airport)
     */
    private String sourceName;

    /**
     * The name of the destination node (airport)
     */
    private String destinationName;

    /**
     * The graph
     */
    private JavaPairRDD<String,Tuple2<String,Double>> graph;

    public SSSP(String source, String destination, JavaRDD<Flight> flights) {
        super(flights);
        this.sourceName = source;
        this.destinationName = destination;
        this.graph = buildGraph();
    }

//    /**
//     * Build the graph from the flights.
//     */
//    private JavaRDD<MatrixEntry> buildGraph2() {
//        JavaPairRDD<Tuple2<Long,Long>, Tuple2<Double,Integer>> flightsPerRoute = flights.mapToPair(
//                f -> new Tuple2<>(new Tuple2<>(f.origInternalId,f.destInternalId),
//                        new Tuple2<>((f.arrtime - f.deptime < 0 ?
//                                f.arrtime - f.deptime + 2400 :
//                                f.arrtime - f.deptime),1)
//                )
//        );
//        JavaPairRDD<Tuple2<Long,Long>,Tuple2<Double,Integer>> flightInfo = flightsPerRoute.reduceByKey(
//                (a,b) -> new Tuple2<>(a._1+b._1,a._2+b._2)
//        );
//        JavaPairRDD<Tuple2<Long,Long>,Double> flightAverageDuration = flightInfo.mapValues(
//                t -> t._1/t._2
//        );
//        // I decided to use the CoordinateMatrix type of distributed type of matrix
//        // Maybe we will see that this type is not the most suitable for our usage
//        JavaRDD<MatrixEntry> entries = flightAverageDuration.map(
//                e -> new MatrixEntry(e._1._1,e._1._2,e._2)
//        ).distinct();
//        return entries;
//    }

    private JavaPairRDD<String,Tuple2<String,Double>> buildGraph(){
        JavaPairRDD<Tuple2<String,String>, Tuple2<Double,Integer>> flightsPerRoute = flights.mapToPair(
                f -> new Tuple2<>(new Tuple2<>(f.origin,f.dest),
                        new Tuple2<>((f.arrtime - f.deptime < 0 ?
                                f.arrtime - f.deptime + 2400 :
                                f.arrtime - f.deptime),1)
                )
        );
        JavaPairRDD<Tuple2<String,String>,Tuple2<Double,Integer>> summed = flightsPerRoute.reduceByKey(
                (a,b) -> new Tuple2<>(a._1+b._1,a._2+b._2)
        );
        JavaPairRDD<Tuple2<String,String>,Double> flightAverageDuration = summed.mapValues(
                f -> (Double) (f._1/f._2)
        );
        return flightAverageDuration.mapToPair(
                e -> new Tuple2<>(e._1._1,new Tuple2<>(e._1._2,e._2))
        );
    }

    public JavaPairRDD<String,Tuple2<String,Double>> getGraph() {
        return graph;
    }

    @Override
    public Route run() {
//        int source = Flight.getAirportIdFromName(sourceName);
//        int destination = Flight.getAirportIdFromName(destinationName);
//        int nAirports = (int) Flight.getNumberAirports();
//
//        // RDD for edges : (src, (dst, weights))
//        JavaPairRDD<Integer, Tuple2<Integer, Double>> edges = graph.mapToPair(
//            e -> new Tuple2<>((int) e.i(), new Tuple2<>((int) e.j(), e.value()))
//        );
//
//        // Distances : (node, distance)
//        JavaPairRDD<Integer, Double> distances = edges
//            .keys()
//            .union(edges.values().map(t -> t._1))
//            .distinct()
//            .mapToPair(node -> new Tuple2<>(node, node == source ? 0.0 : Double.POSITIVE_INFINITY))
//            .cache();
//
//        // RDD for predecessors : (node, distance)
//        JavaPairRDD<Integer, Integer> predecessors = distances.mapToPair(
//            d -> new Tuple2<>(d._1, -1)
//        );
//
//        // Frontier : nodes to explore at this iteration (at beginning, only the source)
//        JavaRDD<Integer> frontier = distances
//            .filter(d -> d._1 == source)
//            .keys();
//
//        while (!frontier.isEmpty()) {
//            // 1) for each node in the frontier, we look at its outgoing neighbors
//            JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Double>, Double>> edgesWithSrcDist =
//                edges.join(distances); // (src, ((dst, weight), srcDist))
//
//            JavaPairRDD<Integer, Tuple2<Double, Integer>> candidateUpdates = edgesWithSrcDist
//                .mapToPair(e -> {
//                    int src = e._1;
//                    int dst = e._2._1._1;
//                    double weight = e._2._1._2;
//                    double srcDist = e._2._2;
//                    double newDist = srcDist + weight;
//                    return new Tuple2<>(dst, new Tuple2<>(newDist, src));
//                });
//
//            // 2) For each neighbor, keep only the smallest proposed distance
//            JavaPairRDD<Integer, Tuple2<Double, Integer>> minUpdates = candidateUpdates
//                .reduceByKey((a, b) -> a._1 < b._1 ? a : b);
//
//            // 3) Update distances and predecessors if a better distance is found
//            JavaPairRDD<Integer, Tuple2<Double, Integer>> joined = distances.join(minUpdates)
//                .filter(t -> t._2._2._1 < t._2._1) // only improvements
//                .mapToPair(t -> new Tuple2<>(t._1, t._2._2)); // (node, (newDist, pred))
//
//            // Break if no updates
//            if (joined.isEmpty()) break;
//
//            // Update distances and predecessors
//            JavaPairRDD<Integer, Double> updatedDistances = joined.mapToPair(t -> new Tuple2<>(t._1, t._2._1));
//            JavaPairRDD<Integer, Integer> updatedPredecessors = joined.mapToPair(t -> new Tuple2<>(t._1, t._2._2));
//
//            distances = distances
//                .leftOuterJoin(updatedDistances)
//                .mapToPair(t -> new Tuple2<>(t._1, t._2._2.orElse(t._2._1)));
//
//            predecessors = predecessors
//                .leftOuterJoin(updatedPredecessors)
//                .mapToPair(t -> new Tuple2<>(t._1, t._2._2.orElse(t._2._1)));
//
//            // Update the frontier
//            frontier = joined.keys();
//        }
//
//        // Get the final distance and predecessors
//        double finalDistance = distances.lookup(destination).isEmpty() ? Double.POSITIVE_INFINITY : distances.lookup(destination).get(0);
//        int[] predArray = new int[nAirports];
//        for (Tuple2<Integer, Integer> t : predecessors.collect()) {
//            predArray[t._1] = t._2;
//        }
//
//        return new Route(source, destination, finalDistance, predArray);
//    }

        final String srcName = sourceName;
        final String dstName = destinationName;

        // RDD for edges : (src, (dst, weights))
        JavaPairRDD<String, Tuple2<String, Double>> edges = graph.cache();
        JavaRDD<String> sources = edges.keys();
        JavaRDD<String> dests = edges.values().map(t -> t._1);
        JavaRDD<String> vertices = sources.union(dests).distinct().cache();

        // distances : (node, distance)
        JavaPairRDD<String, Double> distances = vertices.mapToPair(
                v -> new Tuple2<>(v, v.equals(srcName) ? 0.0 : NO_EDGE)
        ).cache();

        // RDD for predecessors : (node, predecessor)
        JavaPairRDD<String, String> predecessors = vertices.mapToPair(
                v -> new Tuple2<>(v, NO_PREDECESSOR)
        );

        // frontier : nodes to explore at this iteration (at beginning, only the source)
        JavaRDD<String> frontier = vertices.filter(v -> v.equals(srcName));

        while (!frontier.isEmpty()) {

            // 1) for each node in the frontier, we look at its outgoing neighbors
            JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Double>> edgesWithSrcDist =
                    edges.join(distances);

            Broadcast<Set<String>> bFrontier =
                    JavaSparkContext.fromSparkContext(edges.context())
                            .broadcast(new HashSet<>(frontier.collect()));

            // edges filtered to only keep those who are having their source in the frontier
            JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Double>> filtered =
                    edgesWithSrcDist.filter(e -> bFrontier.value().contains(e._1));

            JavaPairRDD<String, Tuple2<Double, String>> candidateUpdates =
                    filtered.mapToPair(e -> {
                        String src = e._1;
                        String dst = e._2._1._1;
                        double weight = e._2._1._2;
                        double srcDist = e._2._2;
                        return new Tuple2<>(dst, new Tuple2<>(srcDist + weight, src));
                    });

            // 2) for each neighbor, keep only the smallest proposed distance
            JavaPairRDD<String, Tuple2<Double, String>> minUpdates =
                    candidateUpdates.reduceByKey((a, b) -> a._1 < b._1 ? a : b);

            // 3) updates distances and predecessors if a better distance is found
            JavaPairRDD<String, Tuple2<Double, String>> improved =
                    distances.join(minUpdates)
                            .filter(t -> (Boolean) (t._2._2._1 < t._2._1))
                            .mapToPair(t -> new Tuple2<>(t._1, t._2._2));

            // break if no update
            if (improved.isEmpty()) break;

            // update distances, predecessors and frontier
            JavaPairRDD<String, Double> updatedDistances =
                    improved.mapToPair(t -> new Tuple2<>(t._1, t._2._1));

            JavaPairRDD<String, String> updatedPredecessors =
                    improved.mapToPair(t -> new Tuple2<>(t._1, t._2._2));

            distances = distances
                    .leftOuterJoin(updatedDistances)
                    .mapToPair(t -> new Tuple2<>(t._1,
                            t._2._2.orElse(t._2._1)));

            predecessors = predecessors
                    .leftOuterJoin(updatedPredecessors)
                    .mapToPair(t -> new Tuple2<>(t._1,
                            t._2._2.orElse(t._2._1)));

            frontier = improved.keys();
        }

        // get the final distances and predecessors
        double finalDistance = distances.lookup(destinationName).isEmpty()
                ? NO_EDGE
                : distances.lookup(destinationName).get(0);

        int nAirports = (int) Flight.getNumberAirports();
        int[] predArray = new int[nAirports];
        Arrays.fill(predArray, -1);

        for (Tuple2<String, String> t : predecessors.collect()) {
            int nodeId = (int) Flight.getAirportIdFromName(t._1);
            String predName = t._2;
            if (!NO_PREDECESSOR.equals(predName) && !predName.isEmpty()) {
                predArray[nodeId] =
                        (int) Flight.getAirportIdFromName(predName);
            }
        }

        int sourceId = (int) Flight.getAirportIdFromName(sourceName);
        int destinationId = (int) Flight.getAirportIdFromName(destinationName);

        return new Route(sourceId, destinationId, finalDistance, predArray);
    }
}
