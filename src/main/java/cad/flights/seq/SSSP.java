package cad.flights.seq;


import cad.flights.AbstractFlightAnalyser;
import cad.flights.Flight;
import cad.flights.Route;
import org.apache.commons.math3.util.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A sequential implementation of Dijkstra Single Source Shortest Path
 */
public class SSSP extends AbstractFlightAnalyser<List<Flight>, Route> {

    /**
     * Representation of absence of edge between two nodes
     */
    private static final double NO_EDGE = Double.MAX_VALUE;

    /**
     * Representation of absence of predecessor for a given node in the current path
     */
    private static final int NO_PREDECESSOR = -1;

    /**
     * The graph
     */
    private final List<MatrixEntry> graph;

    /**
     * The name of the source node (airport)
     */
    private final String sourceName;

    /**
     * The name of the destination node (airport)
     */
    private final String destinationName;


    public SSSP(String source, String destination, List<Flight> flights) {
        super(flights);
        this.sourceName = source;
        this.destinationName = destination;
        this.graph = buildGraph();
    }

    @Override
    public Route run() {
        // identifiers of the source and destination nodes
        int source = Flight.getAirportIdFromName(sourceName);
        int destination = Flight.getAirportIdFromName(destinationName);
        int nAirports = (int) Flight.getNumberAirports();

        // The set of nodes to visit
        List<Integer> toVisit = IntStream.range(0, nAirports).boxed().collect(Collectors.toList());
        toVisit.remove(source);

        // the l vector and a vector to store a node's predecessor in the current path
        double[] l = new double[nAirports];
        int[] predecessor = new int[nAirports];

        for (int i = 0; i < l.length; i++) {
            l[i] = NO_EDGE;
            predecessor[i] = NO_PREDECESSOR;
        }

        l[source] = 0;
        for (Integer v : toVisit) {
            MatrixEntry e = getEdge(source, v);
            if (e != null) {
                l[(int) e.j()] = e.value();
                predecessor[(int) e.j()] = source;
            }
        }

        // Dijkstra's algorithm
        while (!toVisit.isEmpty()) {

            int u = toVisit.get(0);
            for (Integer v : toVisit)
                if (l[v] < l[u]) u = v;

            toVisit.remove((Integer) u);

            for (Integer v : toVisit) {
                double newPath = l[u] + getWeight(u, v);
                if (l[v] > newPath) {
                    l[v] = newPath;
                    predecessor[v] = u;
                }
            }
        }
        return new Route(source, destination, l[destination], predecessor);
    }


    /**
     * Build the graph from the flights.
     */
    private List<MatrixEntry> buildGraph() {

        Map<Pair<Long, Long>, List<Pair<Double, Integer>>> flightsPerRoute = new HashMap<>();
        this.flights.forEach(
                flight -> {
                    var list = flightsPerRoute.computeIfAbsent(new Pair<>(flight.origInternalId, flight.destInternalId),
                            k -> new ArrayList<>());
                    list.add(new Pair<>(flight.arrtime - flight.deptime < 0 ?
                                        flight.arrtime - flight.deptime + 2400 :
                                        flight.arrtime - flight.deptime, 1));
                    });


        Map<Pair<Long, Long>, Pair<Double, Integer>> flightInfo =
                flightsPerRoute.entrySet().stream()
                    .map(entry ->
                        new Pair<>(entry.getKey(),
                                entry.getValue().stream().reduce(
                                    (duration1, duration2) ->
                                            new Pair<>(duration1.getFirst() + duration2.getFirst(),
                                        duration1.getSecond() + duration2.getSecond())).get()))
                    .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

        var flightAverageDuration = flightInfo.entrySet().stream()
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue().getFirst() / entry.getValue().getSecond()))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));;


        return flightAverageDuration.entrySet().stream().map(
                entry -> new MatrixEntry(entry.getKey().getFirst(), entry.getKey().getSecond(), entry.getValue()))
                .collect(Collectors.toList());
    }

    /**
     * Get an edge from between origin and dest, if it exists.
     * @return The edge (of type MatrixEntry), if it exists, null otherwise
     */
    public MatrixEntry getEdge(int origin, int dest) {
        for (MatrixEntry e : this.graph) {
            if (e.i() == origin && e.j() == dest)
                return e;
        }
        return null;
    }

    /**
     * Get the weight of an edge from between origin and dest, if it exists.
     * @return The weight of the edge, if the edge exists, NO_EDGE otherwise
     */
    public double getWeight(int origin, int dest) {
        for (MatrixEntry e : this.graph) {
            if (e.i() == origin && e.j() == dest)
                return e.value();
        }
        return NO_EDGE;
    }

    public List<MatrixEntry> getGraph(){
        return this.graph;
    }
}
