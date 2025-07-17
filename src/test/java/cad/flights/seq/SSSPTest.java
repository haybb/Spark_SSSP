package cad.flights.seq;

import cad.flights.Flight;
import cad.flights.Route;

import java.util.List;

public class SSSPTest extends AbstractFlightsTest<Route> {

	@Override
	protected Route run(List<Flight> flights) {

		long time = System.currentTimeMillis();
		Route route = new SSSP("TPA", "PSG", flights).run();
		long elapsed =  System.currentTimeMillis() - time;
		System.out.println("Route " + route + " with weight " + route.getWeight() + "\nComputed in " + elapsed + " ms.");
		return route;
	}

	@Override
	protected String expectedResult() {
		return "TPA -> MCI -> SEA -> KTN -> WRG -> PSG";
	}	
}
