import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;
import java.util.*;

/**
 * Runs queries against a back-end database.
 * This class is responsible for searching for flights.
 */
public class QuerySearchOnly
{
  // `dbconn.properties` config file
  private String configFilename;

  // DB Connection
  protected Connection conn;

  // To store previous search
  private List<List<Flight>> itinerary;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  protected PreparedStatement checkFlightCapacityStatement;

  // num iter 1
  // originCity 2
  // destination 3
  // day 4
  private static final String CHECK_EVERYTHING_DIRECT =
      "SELECT TOP ( ? ) fid, day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
              + "FROM Flights "
              + "WHERE origin_city = ? AND dest_city = ? AND day_of_month =  ?  and canceled = 0"
              + "ORDER BY actual_time ASC, fid ASC";
  protected PreparedStatement checkEverythingDirectStatement;

  // num iter 1
  // day 2
  // originCity 3
  // destCity 4
  // destCity 5
  private static final String CHECK_EVERYTHING_TWO_HOP =
      "Select TOP ( ? ) f1.fid as fid1, f1.day_of_month as day1, f1.carrier_id as cid1, f1.flight_num as num1, f1.origin_city as o1, "
      + "f1.dest_city as d1, f1.actual_time as time1, f1.capacity as cap1, f1.price as p1, "
      + "f2.fid as fid2, f2.day_of_month as day2, f2.carrier_id as cid2, f2.flight_num as num2, f2.origin_city as o2, "
      + "f2.dest_city as d2, f2.actual_time as time2, f2.capacity as cap2, f2.price as p2 "
      + "from Flights as f1, Flights as f2 "
      + "where f1.day_of_month = ? and f1.day_of_month = F2.day_of_month and "
      + "f1.origin_city = ? and f1.canceled = 0 and f2.canceled = 0 and "
      + "f1.dest_city = F2.origin_city and "
      + "F2.dest_city = ? and f1.actual_time != 0 and "
      + "F2.dest_city not in (select distinct F3.dest_city as c "
                            + "from FLIGHTS F3 "
                            +  "where F3.origin_city = ?) ORDER BY f1.actual_time + F2.actual_time ASC, f1.fid ASC, f2.fid ASC";
  protected PreparedStatement checkEverythingTwoHopStatement;

  class Flight
  {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public int flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString()
    {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
              " Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
              " Capacity: " + capacity + " Price: " + price;
    }
  }

  public QuerySearchOnly(String configFilename)
  {
    this.configFilename = configFilename;
  }

  /** Open a connection to SQL Server in Microsoft Azure.  */
  public void openConnection() throws Exception
  {
    Properties configProps = new Properties();
    configProps.load(new FileInputStream(configFilename));

    String jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
    String jSQLUrl = configProps.getProperty("flightservice.url");
    String jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
    String jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

    /* load jdbc drivers */
    Class.forName(jSQLDriver).newInstance();

    /* open connections to the flights database */
    conn = DriverManager.getConnection(jSQLUrl, // database
            jSQLUser, // user
            jSQLPassword); // password

    conn.setAutoCommit(true); //by default automatically commit after each statement
    /* In the full Query class, you will also want to appropriately set the transaction's isolation level:
          conn.setTransactionIsolation(...)
       See Connection class's JavaDoc for details.
    */
  }

  public void closeConnection() throws Exception
  {
    conn.close();
  }

  /**
   * prepare all the SQL statements in this method.
   * "preparing" a statement is almost like compiling it.
   * Note that the parameters (with ?) are still not filled in
   */
  public void prepareStatements() throws Exception
  {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

    /* add here more prepare statements for all the other queries you need */
    /* . . . . . . */
    checkEverythingDirectStatement = conn.prepareStatement(CHECK_EVERYTHING_DIRECT);
    checkEverythingTwoHopStatement = conn.prepareStatement(CHECK_EVERYTHING_TWO_HOP);
  }

  // returns itinerary number and information
  private String iterNum(int num, int numFlights, int min) {
    String ret = "Itinerary " + num + ": " + numFlights + " flight(s), " + min  + " minutes";
    return ret;
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise it searches for direct flights
   * and flights with two "hops." Only searches for up to the number of
   * itineraries given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your selection\n".
   * If an error occurs, then return "Failed to search\n".
   *
   * Otherwise, the sorted itineraries printed in the following format:
   *
   * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
   * [first flight in itinerary]\n
   * ...
   * [last flight in itinerary]\n
   *
   * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
   * in each search should always start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries)
  {
    // Please implement your own (safe) version that uses prepared statements rather than string concatenation.
    // You may use the `Flight` class (defined above).
    // return transaction_search_unsafe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
    StringBuffer sb = new StringBuffer();
    int counter = 0;

    ArrayList<List<Flight>> flights = new ArrayList<List<Flight>>();

    try
    {
      // prepare the statement
      // this is for direct flights
      checkEverythingDirectStatement.clearParameters();
      checkEverythingDirectStatement.setInt(1, numberOfItineraries);
      checkEverythingDirectStatement.setString(2, originCity);
      checkEverythingDirectStatement.setString(3, destinationCity);
      checkEverythingDirectStatement.setInt(4, dayOfMonth);

      ResultSet results = checkEverythingDirectStatement.executeQuery();
      while (results.next()) {
        Flight f = new Flight();
        f.fid = results.getInt("fid");
        f.dayOfMonth = results.getInt("day_of_month");
        f.carrierId = results.getString("carrier_id");
        f.flightNum = results.getInt("flight_num");
        f.originCity = results.getString("origin_city");
        f.destCity = results.getString("dest_city");
        f.time = results.getInt("actual_time");
        f.capacity = results.getInt("capacity");
        f.price = results.getInt("price");

        List<Flight> iten = new ArrayList<Flight>();
        iten.add(f);
        flights.add(iten);
        counter++;
      }

      results.close();

      // non direct flights
      if (!directFlight) {

        //set parameters
        checkEverythingTwoHopStatement.clearParameters();
        checkEverythingTwoHopStatement.setInt(1, numberOfItineraries - counter);
        checkEverythingTwoHopStatement.setInt(2, dayOfMonth);
        checkEverythingTwoHopStatement.setString(3, originCity);
        checkEverythingTwoHopStatement.setString(4, destinationCity);
        checkEverythingTwoHopStatement.setString(5, destinationCity);

        results = checkEverythingTwoHopStatement.executeQuery();
        while (results.next()) {
          Flight f = new Flight();
          Flight f2 = new Flight();
          int totalTime = 0;

          // set resulting flight parameter
          f.fid = results.getInt("fid1");
          f.dayOfMonth = results.getInt("day1");
          f.carrierId = results.getString("cid1");
          f.flightNum = results.getInt("num1");
          f.originCity = results.getString("o1");
          f.destCity = results.getString("d1");
          f.time = results.getInt("time1");
          f.capacity = results.getInt("cap1");
          f.price = results.getInt("p1");

          f2.fid = results.getInt("fid2");
          f2.dayOfMonth = results.getInt("day2");
          f2.carrierId = results.getString("cid2");
          f2.flightNum = results.getInt("num2");
          f2.originCity = results.getString("o2");
          f2.destCity = results.getString("d2");
          f2.time = results.getInt("time2");
          f2.capacity = results.getInt("cap2");
          f2.price = results.getInt("p2");

          totalTime += f.time;
          totalTime += f2.time;

          List<Flight> iten = new ArrayList<Flight>();
          iten.add(f);
          iten.add(f2);
          flights.add(iten);
        }

        results.close();
      }

    } catch (SQLException e) { e.printStackTrace(); }

    // sort itineraries based off time
    flights.sort(new FCompare());
    if (flights.size() == 0) {
      sb.append("No flights match your selection\n");
    } else {
      sb = flight_string(numberOfItineraries, flights);
    }

    return sb.toString();
  }

  // prints out all flight itineraries
  private StringBuffer flight_string(int lim, ArrayList<List<Flight>> li) {
    int counter = 0;
    StringBuffer sb = new StringBuffer();
    List<List<Flight>> itin = new ArrayList<List<Flight>>();

    for (int i = 0; i < lim && i < li.size(); i++) {
      List<Flight> f = li.get(i);

      itin.add(f);

      int time = 0;

      for (int j = 0; j < f.size(); j++) {
        time += f.get(j).time;
      }
      sb.append(iterNum(i, f.size(), time));
      sb.append("\n");

      for (int j = 0; j < f.size(); j++) {
        sb.append(f.get(j).toString());
        sb.append("\n");
      }
    }
    itinerary = itin;
    return sb;
  }

  public List<List<Flight>> getPrevSearch() {
      return itinerary;
  }

  // compare flight itineraries based on time
  private static class FCompare implements Comparator<List<Flight>> {
    public int compare(List<Flight> f1, List<Flight> f2) {
      int t1 = 0;
      int t2 = 0;
      for (int i = 0; i < f1.size(); i++) {
        t1 += f1.get(i).time;
      }
      for (int j = 0; j < f2.size(); j++) {
        t2 += f2.get(j).time;
      }
      if (t1 != t2) {
        return t1 - t2;
      } else if (f1.get(0).flightNum != f2.get(0).flightNum) {
        return f1.get(0).flightNum - f2.get(0).flightNum;
      } else {
        return f1.get(1).flightNum - f2.get(1).flightNum;
      }
    }
  }

  /**
   * Same as {@code transaction_search} except that it only performs single hop search and
   * do it in an unsafe manner.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight
   * @param dayOfMonth
   * @param numberOfItineraries
   *
   * @return The search results. Note that this implementation *does not conform* to the format required by
   * {@code transaction_search}.
   */
  private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
                                          int dayOfMonth, int numberOfItineraries)
  {
    StringBuffer sb = new StringBuffer();

    try
    {
      // one hop itineraries
      String unsafeSearchSQL =
              "SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
                      + "FROM Flights "
                      + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
                      + "ORDER BY actual_time ASC";

      Statement searchStatement = conn.createStatement();
      ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

      while (oneHopResults.next())
      {
        int result_dayOfMonth = oneHopResults.getInt("day_of_month");
        String result_carrierId = oneHopResults.getString("carrier_id");
        int result_flightNum = oneHopResults.getInt("flight_num");
        String result_originCity = oneHopResults.getString("origin_city");
        String result_destCity = oneHopResults.getString("dest_city");
        int result_time = oneHopResults.getInt("actual_time");
        int result_capacity = oneHopResults.getInt("capacity");
        int result_price = oneHopResults.getInt("price");

        sb.append("Day: ").append(result_dayOfMonth)
                .append(" Carrier: ").append(result_carrierId)
                .append(" Number: ").append(result_flightNum)
                .append(" Origin: ").append(result_originCity)
                .append(" Destination: ").append(result_destCity)
                .append(" Duration: ").append(result_time)
                .append(" Capacity: ").append(result_capacity)
                .append(" Price: ").append(result_price)
                .append('\n');
      }
      oneHopResults.close();
    } catch (SQLException e) { e.printStackTrace(); }
    return sb.toString();
  }

  /**
   * Shows an example of using PreparedStatements after setting arguments.
   * You don't need to use this method if you don't want to.
   */
  private int checkFlightCapacity(int fid) throws SQLException
  {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
}
