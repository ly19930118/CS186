import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class YelpQueries
{
  public static void main(String[] args) throws ClassNotFoundException
  {
    // load the sqlite-JDBC driver using the current class loader
    Class.forName("org.sqlite.JDBC");

    String dbLocation = "yelp_dataset.db"; 

    Connection connection = null;
    try
    {
      // create a database connection
      connection = DriverManager.getConnection("jdbc:sqlite:" + dbLocation);

      Statement statement = connection.createStatement();

      // Question 0
      statement.execute("DROP VIEW IF EXISTS q0"); // Clean out views
      String q0 = "CREATE VIEW q0 AS "
                   + "SELECT count(*) FROM reviews";
      statement.execute(q0);

      // Question 1
      statement.execute("DROP VIEW IF EXISTS q1");
      String q1 = "CREATE VIEW q1 AS " 
                  + "SELECT AVG(U.review_count) FROM users AS U WHERE U.review_count < 10"; // Replace this line
      statement.execute(q1);

      // Question 2
      statement.execute("DROP VIEW IF EXISTS q2");
      String q2 = "CREATE VIEW q2 AS "
                  + "SELECT u.name FROM users u WHERE u.yelping_since > '2014-11' AND u.review_count > 50"; // Replace this line
      statement.execute(q2);

      // Question 3
      statement.execute("DROP VIEW IF EXISTS q3");
      String q3 = "CREATE VIEW q3 AS "
                  + "SELECT b.name, b.stars FROM businesses b WHERE b.city = 'Pittsburgh' AND b.stars > 3"; // Replace this line
      statement.execute(q3);

      // Question 4
      statement.execute("DROP VIEW IF EXISTS q4");
      String q4 = "CREATE VIEW q4 AS "
                  + "SELECT b.name FROM businesses b WHERE b.city = 'Las Vegas' AND b.review_count>=500 ORDER BY b.stars ASC LIMIT 1"; // Replace this line
      statement.execute(q4);

      // Question 5
      statement.execute("DROP VIEW IF EXISTS q5");
      String q5 = "CREATE VIEW q5 AS "
                  + "SELECT b.name FROM businesses b, checkins c WHERE b.business_id = c.business_id AND c.day=0 ORDER BY c.num_checkins DESC LIMIT 5"; // Replace this line
      statement.execute(q5);

      // Question 6
      statement.execute("DROP VIEW IF EXISTS q6");
      String q6 = "CREATE VIEW q6 AS "
                  + "SELECT c.day FROM checkins c GROUP BY c.day ORDER BY SUM(c.checkins) DESC LIMIT 1"; // Replace this line
      statement.execute(q6);

      // Question 7
      statement.execute("DROP VIEW IF EXISTS q7");
      String q7 = "CREATE VIEW q7 AS "
                  + "SELECT b.name FROM businesses b, users u, reviews r WHERE b.business_id = r.business_id AND u.user_id = r.user_id AND u.review_count = (SELECT MAX(u2.review_count) FROM users u2)"; // Replace this line
      statement.execute(q7);

      // Question 8
      statement.execute("DROP VIEW IF EXISTS q8");
      String q8 = "CREATE VIEW q8 AS "
                  + "SELECT AVG(B.stars) FROM businesses B WHERE B.city = 'Edinburgh' AND B.review_count >  (SELECT 0.1*COUNT(b2.review_count) FROM businesses b2)"; // Replace this line
      statement.execute(q8);

      // Question 9
      statement.execute("DROP VIEW IF EXISTS q9");
      String q9 = "CREATE VIEW q9 AS "
                  + "SELECT u.name FROM users u WHERE u.name LIKE '%..%'"; // Replace this line
      statement.execute(q9);

      // Question 10
      statement.execute("DROP VIEW IF EXISTS q10");
      String q10 = "CREATE VIEW q10 AS "
                  + "SELECT b.city FROM businesses b, users u, review r WHERE r.business_id = b.business_id AND r.user_id IN (SELECT u.user_id FROM users u WHERE u.name LIKE '%..%) GROUP BY b.city ORDER BY COUNT(b.review_count) DESC LIMIT 1"; // Replace this line
      statement.execute(q10);

      connection.close();

    }
    catch(SQLException e)
    {
      // if the error message is "out of memory", 
      // it probably means no database file is found
      System.err.println(e.getMessage());
    }
    finally
    {
      try
      {
        if(connection != null)
          connection.close();
      }
      catch(SQLException e)
      {
        // connection close failed.
        System.err.println(e);
      }
    }
  }
}
