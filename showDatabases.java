package query_hive;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class showDatabases {
	static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";  
	static final String JDBC_DB_URL = "jdbc:hive2://hive-server.example.com:10000/default;principal=hive/hive-server.example.com@CLUSTER_REALM;auth=kerberos;kerberosAuthType=fromSubject;ssl=true;sslTrustStore=/home/user/local.truststore;sslTruststorePassword=changeit";
	static String QUERY = "show databases";

	static final String jaasConfigFilePath = "/home/user/jaas.conf";
	static final String USER = null; 
	static final String PASS = null; 
	static final String KERBEROS_REALM = "EXAMPLE.COM";
	static final String KERBEROS_KDC = "KDCHost";
	static final String KERBEROS_PRINCIPAL = "user@EXAMPLE.COM";
	static final String KERBEROS_PASSWORD = "pass";
	
	static Connection getConnection( Subject signedOnUserSubject ) throws Exception{

		Connection conn = (Connection) Subject.doAs(signedOnUserSubject, new PrivilegedExceptionAction<Object>()
				{
			public Object run()
			{    	        	  
				Connection con = null;
				try {
					Class.forName(JDBC_DRIVER);
					con =  DriverManager.getConnection(JDBC_DB_URL,USER,PASS);
				} catch (SQLException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} 
				return con;
			}
				});

		return conn;
	}

	public static class MyCallbackHandler implements CallbackHandler {

		public void handle(Callback[] callbacks)
				throws IOException, UnsupportedCallbackException {
			for (int i = 0; i < callbacks.length; i++) {
				if (callbacks[i] instanceof NameCallback) {
					NameCallback nc = (NameCallback)callbacks[i];
					nc.setName(KERBEROS_PRINCIPAL);
				} else if (callbacks[i] instanceof PasswordCallback) {
					PasswordCallback pc = (PasswordCallback)callbacks[i];
					pc.setPassword(KERBEROS_PASSWORD.toCharArray());
				} else throw new UnsupportedCallbackException
				(callbacks[i], "Unrecognised callback");
			}
		}
	}

	static Subject getSubject() {
		Subject signedOnUserSubject = null;

		// create a LoginContext based on the entry in the login.conf file
		LoginContext lc;
		try {
			lc = new LoginContext("Client", new MyCallbackHandler());
			// login (effectively populating the Subject)
			lc.login();
			// get the Subject that represents the signed-on user
			signedOnUserSubject = lc.getSubject();
		} catch (LoginException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(0);
		}
		return signedOnUserSubject;
	}
	
	private static int  traverseResultSet(ResultSet rs, int max) throws SQLException
	{
		ResultSetMetaData metaData = rs.getMetaData();
		int rowIndex = 0;
		while (rs.next()) {
			for (int i=1; i<=metaData.getColumnCount(); i++) {
				System.out.print("  "  + rs.getString(i));
			}
			System.out.println();
			rowIndex++;	
			if(max > 0 && rowIndex >= max )
				break;
		}
		return rowIndex;
	}
	
	public static void main(String[] args) {
		System.setProperty("java.security.auth.login.config", jaasConfigFilePath);

		try {
			  Class.forName(JDBC_DRIVER);
		    } catch (ClassNotFoundException e) {
		      e.printStackTrace();
		      System.exit(1);
		    }
		 
		System.out.println("-- Test started ---");
		Subject sub = getSubject();

		Connection conn = null;
		try {
			conn = getConnection(sub);
			Statement stmt = conn.createStatement() ;	
			ResultSet rs = stmt.executeQuery( QUERY );
			traverseResultSet(rs, 10);
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			try { if (conn != null) conn.close(); } catch(Exception e) { e.printStackTrace();}
		}

		System.out.println("Test ended  ");
	}
}
