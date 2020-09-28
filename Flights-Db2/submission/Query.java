import java.sql.*;
import java.util.*;

public class Query extends QuerySearchOnly {

	// Logged In User
	private String username; // customer username is unique

	// transactions
	private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	protected PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	protected PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	protected PreparedStatement rollbackTransactionStatement;

	private static final String LOGIN_SQL = "Select * from Users where username = ? and password = ?";
	protected PreparedStatement loginStatement;

	/*
	 1 user
	 2 pass
	 3 balance
	*/
	private static final String CREATE_CUSTOMER = "Insert into Users values(?, ?, ?)";
	protected PreparedStatement createCustomerStatment;

	/*
		1 id int primary key,
		2 username varchar(20),
		3 fid1 int,
		4 fid2 int,
		5 paid int,
		6 day int,
		7 price int,
	*/
	private static final String CREATE_RESERVATION = "Insert into Reservations Values(?, ?, ?, ?, ?, ?, ?)";
	protected PreparedStatement createReservationStatement;

	// count of flights on the day
	// 1 day
	// 2 user
	private static final String SAME_DAY = "Select * from Reservations where day = ? and user = ?";
	protected PreparedStatement sameDayStatement;

	// check reservations for number of reservations for a particular flight
	private static final String CHECK_CURRENT_CAP = "Select COUNT(*) as c from Reservations where fid1 = ? or fid2 = ?";
	protected PreparedStatement checkCurrentCapStatement;

	private static final String FIND_ID = "select rid from id";
	protected PreparedStatement findIDStatement;

	private static final String UPDATE_ID = "delete from id; insert into id values(?)";
	protected PreparedStatement updateIDStatement;

	private static final String CHECK_RESERVATIONS = "Select price, paid from Reservations where id = ? and username = ?";
	protected PreparedStatement checkReservationStatement;

	private static final String CHECK_BALANCE = "Select balance from Users where username = ?";
	protected PreparedStatement checkBalanceStatement;

	private static final String UPDATE_BALANCE = "update Users set balance = ? where username = ?";
	protected PreparedStatement updateBalanceStatement;

	private static final String UPDATE_PAID = "update Reservations set paid = 1 where id = ?";
	protected PreparedStatement updatePaidStatement;

	private static final String RESERVATIONS = "select * from Reservations where username = ?";
	protected PreparedStatement reservationStatement;

	private static final String GET_FLIGHT = "select * from FLIGHTS where fid = ?";
	protected PreparedStatement getFlightStatement;

	private static final String DELETE_USERS = "delete from users";
	protected PreparedStatement deleteUsersStatement;

	private static final String DELETE_ID = "delete from id";
	protected PreparedStatement deleteIDStatement;

	private static final String DELETE_RESERVATIONS = "delete from reservations";
	protected PreparedStatement deleteResStatement;

	private static final String CANCEL = "delete from Reservations where id = ?";
	protected PreparedStatement cancelStatement;

	public Query(String configFilename) {
		super(configFilename);
	}


	/**
	 * Clear the data in any custom tables created. Do not drop any tables and do not
	 * clear the flights table. You should clear any tables you use to store reservations
	 * and reset the next reservation ID to be 1.
	 */
	public void clearTables ()
	{
		// your code here
		try {
			deleteIDStatement.executeUpdate();
			deleteResStatement.executeUpdate();
			deleteUsersStatement.executeUpdate();
		} catch(SQLException e) {}
	}


	/**
	 * prepare all the SQL statements in this method.
	 * "preparing" a statement is almost like compiling it.
	 * Note that the parameters (with ?) are still not filled in
	 */
	@Override
	public void prepareStatements() throws Exception
	{
		super.prepareStatements();
		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
		loginStatement = conn.prepareStatement(LOGIN_SQL);
		createCustomerStatment = conn.prepareStatement(CREATE_CUSTOMER);
		createReservationStatement = conn.prepareStatement(CREATE_RESERVATION);
		sameDayStatement = conn.prepareStatement(SAME_DAY);
		checkCurrentCapStatement = conn.prepareStatement(CHECK_CURRENT_CAP);
		findIDStatement = conn.prepareStatement(FIND_ID);
		updateIDStatement = conn.prepareStatement(UPDATE_ID);
		checkReservationStatement = conn.prepareStatement(CHECK_RESERVATIONS);
		checkBalanceStatement = conn.prepareStatement(CHECK_BALANCE);
		updateBalanceStatement = conn.prepareStatement(UPDATE_BALANCE);
		updatePaidStatement = conn.prepareStatement(UPDATE_PAID);
		reservationStatement = conn.prepareStatement(RESERVATIONS);
		getFlightStatement = conn.prepareStatement(GET_FLIGHT);
		deleteIDStatement = conn.prepareStatement(DELETE_ID);
		deleteResStatement = conn.prepareStatement(DELETE_RESERVATIONS);
		deleteUsersStatement = conn.prepareStatement(DELETE_USERS);
		cancelStatement = conn.prepareStatement(CANCEL);
	}


	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @return If someone has already logged in, then return "User already logged in\n"
	 * For all other errors, return "Login failed\n".
	 *
	 * Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password)
	{
		if (this.username != null) {
			return "User already logged in\n";
		}

		try {
			loginStatement.clearParameters();
			loginStatement.setString(1, username.toLowerCase());
			loginStatement.setString(2, password.toLowerCase());

			ResultSet result = loginStatement.executeQuery();

			// check if username/pass was found
			if (result.next()) {
				this.username = username;
				return "Logged in as " + username + "\n";
			}

		} catch (SQLException e) { e.printStackTrace(); }

		return "Login failed\n";
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username new user's username. User names are unique the system.
	 * @param password new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
	 *
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 */
	public String transaction_createCustomer (String username, String password, int initAmount)
	{
		if (initAmount < 0) {
			return "Failed to create user\n";
		}

		try {
			loginStatement.clearParameters();
			loginStatement.setString(1, username);
			loginStatement.setString(2, password);
			ResultSet res = loginStatement.executeQuery();

			if(res.next()) { 
				res.close();
				return "Failed to create user\n";
			}

			createCustomerStatment.clearParameters();
			createCustomerStatment.setString(1, username);
			createCustomerStatment.setString(2, password);
			createCustomerStatment.setInt(3, initAmount);
			createCustomerStatment.executeUpdate();
		} catch (SQLException e) { return "Failed to create user\n"; }

		return "Created user " + username + "\n";
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
	 *
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
	 * If the user already has a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n".
	 * For all other errors, return "Booking failed\n".
	 *
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
	 * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
	 * successful reservation is made by any user in the system.
	 */
	public String transaction_book(int itineraryId)
	{
		if (username == null) {
			return "Cannot book reservations, not logged in\n";
		}

		List<List<Flight>> itineraries = getPrevSearch();

		// invalid search if id is less than 0 or
		// no previous search is found or
		// not id is not within previous search
		if (itineraryId < 0 || itineraries == null || itineraries.size() - 1 < itineraryId) {
			return "No such itinerary {@code itineraryId}\n";
		}

		try {
			beginTransaction();

			List<Flight> flights = itineraries.get(itineraryId);

			// check if flight has booked in the same day
			sameDayStatement.clearParameters();
			sameDayStatement.setInt(1, flights.get(0).dayOfMonth);
			sameDayStatement.setString(2, this.username);

			ResultSet res = sameDayStatement.executeQuery();
			if (res.next()) {
				res.close();
				rollbackTransaction();
				return "You cannot book two flights in the same day\n";
			}

			res.close();

			// check if capacity is full
			for (Flight f : flights) {
				checkCurrentCapStatement.clearParameters();
				checkCurrentCapStatement.setInt(1, f.fid);
				checkCurrentCapStatement.setInt(2, f.fid);
				res = checkCurrentCapStatement.executeQuery();
				res.next();
				if (res.getInt("c") >= f.capacity) {
					res.close();
					rollbackTransaction();
					return "Booking failed\n";
				}
				res.close();
			}

			// ok everything looks good so far

			// get new id and update id table
			int id;
			res = findIDStatement.executeQuery();

			// if table is empty this is the first entry
			if (res.next()) {
				id = res.getInt("rid");
			} else {
				id = 1; // if empty first reservation is 1
			}

			res.close();

			// update table for next reservation
			updateIDStatement.clearParameters();
			updateIDStatement.setInt(1, id + 1);
			updateIDStatement.executeUpdate();

			// create reservation
			int price = flights.get(0).price;
			createReservationStatement.clearParameters();
			createReservationStatement.setInt(1, id); // id
			createReservationStatement.setString(2, this.username);
			createReservationStatement.setInt(3, flights.get(0).fid); // fid1

			if (flights.size() == 1) {
				createReservationStatement.setNull(4, 0); // fid2
			} else {
				price += flights.get(1).price;
				createReservationStatement.setInt(4, flights.get(1).fid); // fid2
			}

			createReservationStatement.setInt(5, 0); // not paid
			createReservationStatement.setInt(6, flights.get(0).dayOfMonth); // day
			createReservationStatement.setInt(7, price);
			createReservationStatement.executeUpdate();
			commitTransaction();

			return "Booked flight(s), reservation ID: " + id + "\n";

		} catch (SQLException e) {
			try {
				rollbackTransaction();
			} catch (SQLException a) { a.printStackTrace(); }
			return "Booking failed\n";
		}
	}

	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 *
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n"
	 * If the reservation is not found / not under the logged in user's name, then return
	 * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
	 * If the user does not have enough money in their account, then return
	 * "User has only [balance] in account but itinerary costs [cost]\n"
	 * For all other errors, return "Failed to pay for reservation [reservationId]\n"
	 *
	 * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
	 * where [balance] is the remaining balance in the user's account.
	 */
	public String transaction_pay (int reservationId)
	{
		if (this.username == null) {
			return "Cannot pay, not logged in\n";
		}

		try {
			beginTransaction();

			checkReservationStatement.clearParameters();
			checkReservationStatement.setInt(1, reservationId);
			checkReservationStatement.setString(2, this.username);

			ResultSet res = checkReservationStatement.executeQuery();

			if (!res.next()) {
				rollbackTransaction();
				return "Cannot find unpaid reservation " + reservationId + " under user: "+ username + "\n";
			}

			int price = res.getInt("price");
			int paid = res.getInt("paid");

			if (paid == 1) {
				rollbackTransaction();
				return "Cannot find unpaid reservation " + reservationId + " under user: "+ username + "\n";
			}

			// check balance
			checkBalanceStatement.clearParameters();
			checkBalanceStatement.setString(1, this.username);
			res = checkBalanceStatement.executeQuery();
			res.next();

			int balance = res.getInt("balance");

			res.close();

			if (price > balance) {
				rollbackTransaction();
				return "User has only " + balance + " in account but itinerary costs " + price + "\n";
			}

			// update balance
			updateBalanceStatement.clearParameters();
			updateBalanceStatement.setInt(1, balance - price);
			updateBalanceStatement.setString(2, this.username);
			updateBalanceStatement.executeUpdate();

			// update the reservation to paid
			updatePaidStatement.clearParameters();
			updatePaidStatement.setInt(1, reservationId);
			updatePaidStatement.executeUpdate();

			commitTransaction();
			return "Paid reservation: " + reservationId + " remaining balance: " + (balance - price) + "\n";

		} catch (SQLException e) {
						e.printStackTrace();
			try {
				rollbackTransaction();
			} catch (SQLException a) { a.printStackTrace(); }

			return "Failed to pay for reservation " + reservationId + "\n";
		}
		//return "Failed to pay for reservation " + reservationId + "\n";
	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
	 * If the user has no reservations, then return "No reservations found\n"
	 * For all other errors, return "Failed to retrieve reservations\n"
	 *
	 * Otherwise return the reservations in the following format:
	 *
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * ...
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 *
	 * @see Flight#toString()
	 */
	public String transaction_reservations()
	{
		if (this.username == null) {
			return "Cannot view reservations, not logged in\n";
		}

		StringBuffer sb = new StringBuffer();

		try {
			reservationStatement.clearParameters();
			reservationStatement.setString(1, this.username);

			ResultSet res = reservationStatement.executeQuery();

			while (res.next()) {
				int resID = res.getInt("id");
				int paid = res.getInt("paid");

				sb.append("Reservation " + resID + " paid: ");
				if (paid == 0) {
					sb.append("false:\n");
				} else {
					sb.append("true:\n");
				}

				int fid = res.getInt("fid1");
				getFlightStatement.clearParameters();
				getFlightStatement.setInt(1, fid);
				ResultSet fly = getFlightStatement.executeQuery();
				fly.next();

				Flight f = new Flight();
				f.fid = fly.getInt("fid");
				f.dayOfMonth = fly.getInt("day_of_month");
				f.carrierId = fly.getString("carrier_id");
				f.flightNum = fly.getInt("flight_num");
				f.originCity = fly.getString("origin_city");
				f.destCity = fly.getString("dest_city");
				f.time = fly.getInt("actual_time");
				f.capacity = fly.getInt("capacity");
				f.price = fly.getInt("price");

				sb.append(f.toString());
				sb.append("\n");

				fly.close();

				fid = res.getInt("fid2");
				getFlightStatement.clearParameters();
				getFlightStatement.setInt(1, fid);
				fly = getFlightStatement.executeQuery();

				if (fly.next()) {
					f.fid = fly.getInt("fid");
					f.dayOfMonth = fly.getInt("day_of_month");
					f.carrierId = fly.getString("carrier_id");
					f.flightNum = fly.getInt("flight_num");
					f.originCity = fly.getString("origin_city");
					f.destCity = fly.getString("dest_city");
					f.time = fly.getInt("actual_time");
					f.capacity = fly.getInt("capacity");
					f.price = fly.getInt("price");

					sb.append(f.toString());
					sb.append("\n");
					fly.close();
				}
			}
			res.close();

			if (sb.length() == 0) {
				return "No reservations found\n";
			}

			return sb.toString();
		} catch (SQLException e) {
			return "Failed to retrieve reservations\n";
		}
	}

	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 *
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
	 * For all other errors, return "Failed to cancel reservation [reservationId]"
	 *
	 * If successful, return "Canceled reservation [reservationId]"
	 *
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId)
	{
		// only implement this if you are interested in earning extra credit for the HW!
		if (username == null) {
			return "Cannot cancel reservations, not logged in\n";
		}

		try {
			beginTransaction();

			checkReservationStatement.clearParameters();
			checkReservationStatement.setInt(1, reservationId);
			checkReservationStatement.setString(2, this.username);

			ResultSet res = checkReservationStatement.executeQuery();

			if (!res.next()) {
				rollbackTransaction();
				return "Failed to cancel reservation " + reservationId + "\n";
			}

			int paid = res.getInt("paid");
			int price = res.getInt("price");

			// if paid we give a refund
			if (paid == 1) {
				checkBalanceStatement.clearParameters();
				checkBalanceStatement.setString(1, this.username);
				res = checkBalanceStatement.executeQuery();
				res.next();
				int balance = res.getInt("balance");

				updateBalanceStatement.clearParameters();
				updateBalanceStatement.setInt(1, balance + price);
				updateBalanceStatement.setString(2, this.username);
				updateBalanceStatement.executeUpdate();
			}

			cancelStatement.clearParameters();
			cancelStatement.setInt(1, reservationId);
			cancelStatement.executeUpdate();

			res.close();
			commitTransaction();
			return "Canceled reservation " + reservationId + "\n";
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				rollbackTransaction();
			} catch (SQLException a) { a.printStackTrace(); }
			return "Failed to cancel reservation " + reservationId + "\n";
		}
	}


	/* some utility functions below */

	public void beginTransaction() throws SQLException
	{
		conn.setAutoCommit(false);
		beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException
	{
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException
	{
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}
}
