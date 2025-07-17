package cad.flights;


public abstract class AbstractFlightAnalyser<Flights, T> {

	protected final Flights flights;
	
	public AbstractFlightAnalyser(Flights flights) {
		this.flights = flights;
	}

	public abstract T run();
}
