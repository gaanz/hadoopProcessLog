package garn.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
public class ProcessLogService {
	
	private static final Logger LOG = Logger.getLogger(ProcessLogService.class);
	
	public static class CountSerivceLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static String START = "Start";
		private final static String END = "End";
		private final static String PIPE = "\\|";
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			LOG.info("Start ServiceLog "+key);
			// JDBC driver name and database URL
			String DB_URL = "jdbc:oracle:thin:@10.104.244.32:1521:eservdev";

			// Database credentials
			String USER = "eserv3g";
			String PASS = "eserv3g";

			Connection conn = null;
			Statement stmt = null;
			try {

				Class.forName("oracle.jdbc.OracleDriver");

				java.util.Properties info = new java.util.Properties();
				info.put("user", USER);
				info.put("password", PASS);
				info.put("useUnicode", "true");
				info.put("characterEncoding", "UTF-8");

				conn = DriverManager.getConnection(DB_URL, info);
				conn.setAutoCommit(false);
				LOG.info(" Connect DB Success "+key);
				String lines = value.toString();
			    String []lineArr = lines.split("\n");
			    int lcount = lineArr.length;
			    LOG.info(" LineCount : "+lcount);
			    String[] rowServiceLog = null;
				for (int i = 0; i < lcount; i++) {
					if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
						try {
							
							String rowServiceLogs = lineArr[i];
							//String rowServiceLogs = "2015011416:44:30|room-nameappxx|ESERV3G|0|SERVICELOG|SSID=lx7i3pd5k04etj03|AUDIT_LOG_ID=|INVOKE=501_04cc317a-dad6-45e5-914a-751e4f638d7f|INVOKE_PARENT=501|EVENT_LOG_ID=null|EVENT_LOG_NAME=null|SERVICE_ID=5|SERVICE_SYSTEM=SSBAPI|SERVICE_NAME=getNType|REQUEST_MSG=66923350140|REQUEST_DATE=14012015 16:44:30 PM|RESPONSE_CODE=Y|RESPONSE_MSG={\"detail\":{\"networkType\":\"3PE\",\"mobileLocation\":\"Non BOS\",\"spName\":\"awn\",\"chargeMode\":\"1\"},\"resultCode\":\"20000\",\"developerMessage\":\"Ntype Match Success\"}|RESPONSE_DATE=14012015 16:44:30 PM|RESPONSE_FLAG=Y|MOBILE_NO=0923350140|NTYPE=3PE|ADMIN_FLAG=|CHANNEL=eService";
							rowServiceLog = rowServiceLogs.split(PIPE);
							
							
							String LOGTIME = ModifyInput(rowServiceLog[0],"LOGTIME");					
							String SSID = ModifyInput(rowServiceLog[5],"SSID");
							String AUDIT_LOG_ID = ModifyInput(rowServiceLog[6],"AUDIT_LOG_ID");
							String INVOKE = ModifyInput(rowServiceLog[7],"INVOKE");
							String INVOKE_PARENT = ModifyInput(rowServiceLog[8],"INVOKE_PARENT");
							String EVENT_LOG_ID = ModifyInput(rowServiceLog[9],"EVENT_LOG_ID");
							String EVENT_LOG_NAME =  ModifyInput(rowServiceLog[10],"EVENT_LOG_NAME");
							String SERVICE_ID = ModifyInput(rowServiceLog[11],"SERVICE_ID");
							String SERVICE_SYSTEM = ModifyInput(rowServiceLog[12],"SERVICE_SYSTEM");
							String SERVICE_NAME = ModifyInput(rowServiceLog[13],"SERVICE_NAME");
							String REQUEST_MSG = ModifyInput(rowServiceLog[14],"REQUEST_MSG");
							String REQUEST_DATE = ModifyInput(rowServiceLog[15],"REQUEST_DATE");
							String RESPONSE_CODE = ModifyInput(rowServiceLog[16],"RESPONSE_CODE");
							String RESPONSE_MSG = ModifyInput(rowServiceLog[17],"RESPONSE_MSG");
							String RESPONSE_DATE = ModifyInput(rowServiceLog[18],"RESPONSE_DATE");
							String RESPONSE_FLAG = ModifyInput(rowServiceLog[19],"RESPONSE_FLAG");
							String MOBILE_NO = ModifyInput(rowServiceLog[20],"MOBILE_NO");
							String NTYPE = ModifyInput(rowServiceLog[21],"NTYPE");
							String ADMIN_FLAG = ModifyInput(rowServiceLog[22],"ADMIN_FLAG");
							String CHANNEL = ModifyInput(rowServiceLog[23],"CHANNEL");

						

							stmt = conn.createStatement();						
							
							String sql = " INSERT INTO SSB_ES_LOG_SERVICE VALUES( SSB_ES_LOG_SERVICE_SEQ.NEXTVAL , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? ,TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF'), ? , ? , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , ? , ? , ? , ? , ?) ";
							PreparedStatement st = conn.prepareStatement(sql);
							st.setString(1,LOGTIME);
							st.setString(2,SSID);
							st.setString(3,AUDIT_LOG_ID);
							st.setString(4,INVOKE);
							st.setString(5,INVOKE_PARENT);
							st.setString(6,EVENT_LOG_ID);
							st.setString(7,EVENT_LOG_NAME);
							st.setString(8,SERVICE_ID);
							st.setString(9,SERVICE_SYSTEM);
							st.setString(10,SERVICE_NAME);
							st.setString(11,REQUEST_MSG);
							st.setString(12,REQUEST_DATE);
							st.setString(13,RESPONSE_CODE);
							st.setString(14,RESPONSE_MSG);
							st.setString(15,RESPONSE_DATE);
							st.setString(16,RESPONSE_FLAG);
							st.setString(17,MOBILE_NO);
							st.setString(18,NTYPE);
							st.setString(19,ADMIN_FLAG);
							st.setString(20,CHANNEL);

				
							st.execute();
							conn.commit();
							word.set("Success");
							context.write(word, one);
							LOG.info(" Insert DB Success "+key);
						} catch (SQLException se) {
							// Handle errors for JDBC
							se.printStackTrace();
							LOG.info(se);
							LOG.info("DATA : "+rowServiceLog);
							
							if (conn != null) {
								try {
									conn.rollback();
									word.set("Fail");
									context.write(word, one);
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
									
								}
							}

						} catch (Exception e) {
							// Handle errors for Class.forName
							e.printStackTrace();
							LOG.info(e);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									word.set("Fail");
									context.write(word, one);
									conn.rollback();

								} catch (SQLException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}

						}
					}
					
				}

			} catch (SQLException se) {
				// Handle errors for JDBC
				se.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			} catch (Exception e) {
				// Handle errors for Class.forName
				e.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			} finally {
				// finally block used to close resources
				try {
					if (stmt != null)
						conn.close();
				} catch (SQLException se) {
				}// do nothing
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}
	}
	
	public static class CountIncomingLogMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static String START = "Start";
		private final static String END = "End";
		private final static String PIPE = "\\|";
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// JDBC driver name and database URL
			String DB_URL = "jdbc:oracle:thin:@10.252.240.245:1521:campaigndb";

			// Database credentials
			String USER = "neweServ";
			String PASS = "neweServ";

			Connection conn = null;
			Statement stmt = null;
			try {

				Class.forName("oracle.jdbc.OracleDriver");

				java.util.Properties info = new java.util.Properties();
				info.put("user", USER);
				info.put("password", PASS);
				info.put("useUnicode", "true");
				info.put("characterEncoding", "UTF-8");

				conn = DriverManager.getConnection(DB_URL, info);
				conn.setAutoCommit(false);

				String lines = value.toString();
			    String []lineArr = lines.split("\n");
			    int lcount = lineArr.length;
			    String[] rowServiceLog = null;
				for (int i = 0; i < lcount; i++) {
					if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
						try {
							
							String rowServiceLogs = lineArr[i];
							
							rowServiceLog = rowServiceLogs.split(PIPE);
											
							String LOGTIME = ModifyInput(rowServiceLog[0],"LOGTIME");						
							String SSID= ModifyInput(rowServiceLog[5],"SSID");
							String AUDIT_LOG_ID = ModifyInput(rowServiceLog[6],"AUDIT_LOG_ID");
							String EVENT_LOG_ID = ModifyInput(rowServiceLog[7],"EVENT_LOG_ID");
							String EVENT_LOG_NAME = ModifyInput(rowServiceLog[8],"EVENT_LOG_NAME");
							String INVOKE=ModifyInput(rowServiceLog[9],"INVOKE");
							String COMMAND=ModifyInput(rowServiceLog[10],"COMMAND");
							String DATA=ModifyInput(rowServiceLog[11],"DATA");
					

							stmt = conn.createStatement();						
					
							String sql = " INSERT INTO GARN_LOG_INCOMING VALUES( GARN_LOG_INCOMING_SEQ.NEXTVAL , TO_TIMESTAMP(?, 'yyyymmddhh24:mi:ss') , ? , ? , ? , ? ,? , ? , ? )";
							PreparedStatement st = conn.prepareStatement(sql);
							st.setString(1,LOGTIME);
							st.setString(2,SSID);
							st.setString(3,AUDIT_LOG_ID);
							st.setString(4,EVENT_LOG_ID);
							st.setString(5,EVENT_LOG_NAME);						
							st.setString(6,INVOKE);
							st.setString(7,COMMAND);
							st.setString(8,DATA);
						
							st.execute();
							conn.commit();
							word.set("Success");
							context.write(word, one);
						} catch (SQLException se) {
							// Handle errors for JDBC
							se.printStackTrace();
							LOG.info(se);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									conn.rollback();
									word.set("Fail");
									context.write(word, one);
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

						} catch (Exception e) {
							// Handle errors for Class.forName
							e.printStackTrace();
							LOG.info(e);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									word.set("Fail");
									context.write(word, one);
									conn.rollback();

								} catch (SQLException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}

						}
					}
					
				}

			} catch (SQLException se) {
				// Handle errors for JDBC
				se.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			} catch (Exception e) {
				// Handle errors for Class.forName
				e.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			} finally {
				// finally block used to close resources
				try {
					if (stmt != null)
						conn.close();
				} catch (SQLException se) {
				}// do nothing
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}
	}
	
	public static class CountDetailLogMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static String START = "Start";
		private final static String END = "End";
		private final static String PIPE = "\\|";
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// JDBC driver name and database URL
			String DB_URL = "jdbc:oracle:thin:@10.252.240.245:1521:campaigndb";

			// Database credentials
			String USER = "neweServ";
			String PASS = "neweServ";

			Connection conn = null;
			Statement stmt = null;
			try {

				Class.forName("oracle.jdbc.OracleDriver");

				java.util.Properties info = new java.util.Properties();
				info.put("user", USER);
				info.put("password", PASS);
				info.put("useUnicode", "true");
				info.put("characterEncoding", "UTF-8");

				conn = DriverManager.getConnection(DB_URL, info);
				conn.setAutoCommit(false);

				String lines = value.toString();
			    String []lineArr = lines.split("\n");
			    int lcount = lineArr.length;
			    String[] rowServiceLog =  null;
				for (int i = 0; i < lcount; i++) {
					if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
						try {
							
							String rowServiceLogs = lineArr[i];
							rowServiceLog = rowServiceLogs.split(PIPE);
							
							
							
							String DETAIL_TYPE = ModifyInput(rowServiceLog[4] ,"DETAIL_TYPE");
							String STATE = ModifyInput(rowServiceLog[5],"STATE");
							String IN_TIME = ModifyInput(rowServiceLog[6],"IN_TIME");
							String OUT_TIME = ModifyInput(rowServiceLog[7],"OUT_TIME");
							String DIFF_TIME = ModifyInput(rowServiceLog[8],"DIFF_TIME");
							String SSID = ModifyInput(rowServiceLog[9],"SSID");
							String INVOKE_PARENT = ModifyInput(rowServiceLog[10],"INVOKE_PARENT");
							String INVOKE = ModifyInput(rowServiceLog[11],"INVOKE");
							String MOBILE_NO = ModifyInput(rowServiceLog[12],"MOBILE_NO");
							String RESULTCODE = ModifyInput(rowServiceLog[14],"RESULT_CODE");
							String RESULTDESC = ModifyInput(rowServiceLog[15],"RESULT_DESC");
							String AUDITLOGID = ModifyInput(rowServiceLog[16],"AUDIT_LOG_ID");
							String CHANNEL = ModifyInput(rowServiceLog[17],"CHANNEL");


						

							stmt = conn.createStatement();						
							
							String sql = " INSERT INTO GARN_LOG_DETAIL VALUES( GARN_LOG_DETAIL_SEQ.NEXTVAL , ? , ? , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , ? , ? , ? , ? , ? , ? , ? ,? , ? ) ";
							PreparedStatement st = conn.prepareStatement(sql);
							st.setString(1,DETAIL_TYPE);
							st.setString(2,STATE);
							st.setString(3,IN_TIME);
							st.setString(4,OUT_TIME);
							st.setString(5,DIFF_TIME);
							st.setString(6,SSID);
							st.setString(7,INVOKE_PARENT);
							st.setString(8,INVOKE);
							st.setString(9,MOBILE_NO);
							st.setString(10,RESULTCODE);
							st.setString(11,RESULTDESC);
							st.setString(12,AUDITLOGID);
							st.setString(13,CHANNEL);
							

				
							st.execute();
							conn.commit();
							word.set("Success");
							context.write(word, one);
						} catch (SQLException se) {
							// Handle errors for JDBC
							se.printStackTrace();
							LOG.info(se);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									conn.rollback();
									word.set("Fail");
									context.write(word, one);
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

						} catch (Exception e) {
							// Handle errors for Class.forName
							e.printStackTrace();
							LOG.info(e);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									word.set("Fail");
									context.write(word, one);
									conn.rollback();

								} catch (SQLException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}

						}
					}
					
				}

			} catch (SQLException se) {
				// Handle errors for JDBC
				se.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			} catch (Exception e) {
				// Handle errors for Class.forName
				e.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			} finally {
				// finally block used to close resources
				try {
					if (stmt != null)
						conn.close();
				} catch (SQLException se) {
				}// do nothing
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}
	}

	public static class CountSummaryLogMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static String START = "Start";
		private final static String END = "End";
		private final static String PIPE = "\\|";
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// JDBC driver name and database URL
			String DB_URL = "jdbc:oracle:thin:@10.252.240.245:1521:campaigndb";

			// Database credentials
			String USER = "neweServ";
			String PASS = "neweServ";

			Connection conn = null;
			Statement stmt = null;
			try {

				Class.forName("oracle.jdbc.OracleDriver");

				java.util.Properties info = new java.util.Properties();
				info.put("user", USER);
				info.put("password", PASS);
				info.put("useUnicode", "true");
				info.put("characterEncoding", "UTF-8");

				conn = DriverManager.getConnection(DB_URL, info);
				conn.setAutoCommit(false);

				String lines = value.toString();
			    String []lineArr = lines.split("\n");
			    int lcount = lineArr.length;
			    String[] rowServiceLog = null;
				for (int i = 0; i < lcount; i++) {
					if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
						try {
							
							String rowServiceLogs = lineArr[i];
							//String rowServiceLogs = "2015011416:44:30|room-nameappxx|ESERV3G|0|SERVICELOG|SSID=lx7i3pd5k04etj03|AUDIT_LOG_ID=|INVOKE=501_04cc317a-dad6-45e5-914a-751e4f638d7f|INVOKE_PARENT=501|EVENT_LOG_ID=null|EVENT_LOG_NAME=null|SERVICE_ID=5|SERVICE_SYSTEM=SSBAPI|SERVICE_NAME=getNType|REQUEST_MSG=66923350140|REQUEST_DATE=14012015 16:44:30 PM|RESPONSE_CODE=Y|RESPONSE_MSG={\"detail\":{\"networkType\":\"3PE\",\"mobileLocation\":\"Non BOS\",\"spName\":\"awn\",\"chargeMode\":\"1\"},\"resultCode\":\"20000\",\"developerMessage\":\"Ntype Match Success\"}|RESPONSE_DATE=14012015 16:44:30 PM|RESPONSE_FLAG=Y|MOBILE_NO=0923350140|NTYPE=3PE|ADMIN_FLAG=|CHANNEL=eService";
							rowServiceLog = rowServiceLogs.split(PIPE);
							
							
							
							String IN_TIME = ModifyInput(rowServiceLog[5],"IN_TIME");
							String OUT_TIME = ModifyInput(rowServiceLog[6],"OUT_TIME");
							String DIFF_TIME = ModifyInput(rowServiceLog[7],"DIFF_TIME");
							String SSID = ModifyInput(rowServiceLog[8],"SSID");
							String INVOKE = ModifyInput(rowServiceLog[9],"INVOKE");
							String MOBILE_NO = ModifyInput(rowServiceLog[10],"MOBILE_NO");
							String INPUT = ModifyInput(rowServiceLog[11],"INPUT");
							String OUTPUT = ModifyInput(rowServiceLog[12],"OUTPUT");
							String STATUS = ModifyInput(rowServiceLog[13],"STATUS");
							String RESULTCODE = ModifyInput(rowServiceLog[14],"RESULT_CODE");
							String RESULTDESC = ModifyInput(rowServiceLog[15],"RESULT_DESC");
							String AUDITLOGID = ModifyInput(rowServiceLog[16],"AUDIT_LOG_ID");


						

							stmt = conn.createStatement();						
							
							String sql = " INSERT INTO GARN_LOG_SUMMARY VALUES( GARN_LOG_SUMMARY_SEQ.NEXTVAL , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? ) ";
							PreparedStatement st = conn.prepareStatement(sql);
							st.setString(1,IN_TIME);
							st.setString(2,OUT_TIME);
							st.setString(3,DIFF_TIME);
							st.setString(4,SSID);
							st.setString(5,INVOKE);
							st.setString(6,MOBILE_NO);
							st.setString(7,INPUT);
							st.setString(8,OUTPUT);
							st.setString(9,STATUS);
							st.setString(10,RESULTCODE);
							st.setString(11,RESULTDESC);
							st.setString(12,AUDITLOGID);
							
							st.execute();
							conn.commit();
							word.set("Success");
							context.write(word, one);
						} catch (SQLException se) {
							// Handle errors for JDBC
							se.printStackTrace();
							LOG.info(se);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									conn.rollback();
									word.set("Fail");
									context.write(word, one);
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

						} catch (Exception e) {
							// Handle errors for Class.forName
							e.printStackTrace();
							LOG.info(e);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									word.set("Fail");
									context.write(word, one);
									conn.rollback();

								} catch (SQLException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}

						}
					}
					
				}

			} catch (SQLException se) {
				// Handle errors for JDBC
				se.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			} catch (Exception e) {
				// Handle errors for Class.forName
				e.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			} finally {
				// finally block used to close resources
				try {
					if (stmt != null)
						conn.close();
				} catch (SQLException se) {
				}// do nothing
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}
	}
	
	
	public static class CountEventLogMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static String START = "Start";
		private final static String END = "End";
		private final static String PIPE = "\\|";
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// JDBC driver name and database URL
			String DB_URL = "jdbc:oracle:thin:@10.104.244.32:1521:eservdev";

			// Database credentials
			String USER = "eserv3g";
			String PASS = "eserv3g";

			Connection conn = null;
			Statement stmt = null;
			try {

				Class.forName("oracle.jdbc.OracleDriver");

				java.util.Properties info = new java.util.Properties();
				info.put("user", USER);
				info.put("password", PASS);
				info.put("useUnicode", "true");
				info.put("characterEncoding", "UTF-8");

				conn = DriverManager.getConnection(DB_URL, info);
				conn.setAutoCommit(false);

				String lines = value.toString();
			    String []lineArr = lines.split("\n");
			    int lcount = lineArr.length;
			    String[] rowServiceLog = null;
				for (int i = 0; i < lcount; i++) {
					if(!START.equals(lineArr[i]) && !END.equals(lineArr[i])){
						try {
							
							String rowServiceLogs = lineArr[i];
							//String rowServiceLogs = "2015011416:44:30|room-nameappxx|ESERV3G|0|SERVICELOG|SSID=lx7i3pd5k04etj03|AUDIT_LOG_ID=|INVOKE=501_04cc317a-dad6-45e5-914a-751e4f638d7f|INVOKE_PARENT=501|EVENT_LOG_ID=null|EVENT_LOG_NAME=null|SERVICE_ID=5|SERVICE_SYSTEM=SSBAPI|SERVICE_NAME=getNType|REQUEST_MSG=66923350140|REQUEST_DATE=14012015 16:44:30 PM|RESPONSE_CODE=Y|RESPONSE_MSG={\"detail\":{\"networkType\":\"3PE\",\"mobileLocation\":\"Non BOS\",\"spName\":\"awn\",\"chargeMode\":\"1\"},\"resultCode\":\"20000\",\"developerMessage\":\"Ntype Match Success\"}|RESPONSE_DATE=14012015 16:44:30 PM|RESPONSE_FLAG=Y|MOBILE_NO=0923350140|NTYPE=3PE|ADMIN_FLAG=|CHANNEL=eService";
							rowServiceLog = rowServiceLogs.split(PIPE);
							
							String SSID = ModifyInput(rowServiceLog[5],"SSID");
							String AUDIT_LOG_ID = ModifyInput(rowServiceLog[6],"AUDIT_LOG_ID");
							String EVENT_LOG_ID = ModifyInput(rowServiceLog[7],"EVENT_LOG_ID");
							String EVENT_LOG_NAME = ModifyInput(rowServiceLog[8],"EVENT_LOG_NAME");
							String EVENT_LOG_DATE = ModifyInput(rowServiceLog[9],"EVENT_LOG_DATE");
							String EVENT_LOG_TYPE = ModifyInput(rowServiceLog[10],"EVENT_LOG_TYPE");
							String USER_TYPE = ModifyInput(rowServiceLog[11],"USER_TYPE");
							String NTYPE = ModifyInput(rowServiceLog[12],"NTYPE");
							String LANG = ModifyInput(rowServiceLog[13],"LANG");	

							stmt = conn.createStatement();						
							
							String sql = " INSERT INTO SSB_ES_LOG_EVENT VALUES( SSB_ES_LOG_EVENT_SEQ.NEXTVAL  , ? , ? , ? , ?,TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF'),?, ?, ?, ? ) ";
							PreparedStatement st = conn.prepareStatement(sql);
							st.setString(1,SSID);
							st.setString(2,AUDIT_LOG_ID);
							st.setString(3,EVENT_LOG_ID);
							st.setString(4,EVENT_LOG_NAME);
							st.setString(5,EVENT_LOG_DATE);
							st.setString(6,EVENT_LOG_TYPE);
							st.setString(7,USER_TYPE);
							st.setString(8,NTYPE);
							st.setString(9,LANG);
						

							st.execute();
							conn.commit();
							word.set("Success");
							context.write(word, one);
						} catch (SQLException se) {
							// Handle errors for JDBC
							se.printStackTrace();
							LOG.info(se);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									conn.rollback();
									word.set("Fail");
									context.write(word, one);
								} catch (SQLException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}

						} catch (Exception e) {
							// Handle errors for Class.forName
							e.printStackTrace();
							LOG.info(e);
							LOG.info("DATA : "+rowServiceLog);
							if (conn != null) {
								try {
									word.set("Fail");
									context.write(word, one);
									conn.rollback();

								} catch (SQLException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}

						}
					}
					
				}

			} catch (SQLException se) {
				// Handle errors for JDBC
				se.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

			} catch (Exception e) {
				// Handle errors for Class.forName
				e.printStackTrace();
				if (conn != null) {
					try {
						conn.rollback();
						word.set("Fail");
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			} finally {
				// finally block used to close resources
				try {
					if (stmt != null)
						conn.close();
				} catch (SQLException se) {
				}// do nothing
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException se) {
					se.printStackTrace();
				}
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ProcessLogService");
		job.setJarByClass(ProcessLogService.class);
		job.setMapperClass(CountSerivceLogMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logservice"));
		job.setInputFormatClass(NLinesInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logservice"));
		
		
	    job.waitForCompletion(true);
	    
//	    job = Job.getInstance(conf, "ProcessLogIncoming");
//		job.setJarByClass(ProcessLogService.class);
//		job.setMapperClass(CountIncomingLogMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logincoming"));
//		job.setInputFormatClass(NLinesInputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logincoming"));
//			  
//	    job.waitForCompletion(true);
//	    
//	    job = Job.getInstance(conf, "ProcessLogDetail");
//		job.setJarByClass(ProcessLogService.class);
//		job.setMapperClass(CountDetailLogMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logdetail"));
//		job.setInputFormatClass(NLinesInputFormat.class);
//		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logdetail"));
//			  
//	    job.waitForCompletion(true);
//	    
//	    job = Job.getInstance(conf, "ProcessLogSummary");
//  		job.setJarByClass(ProcessLogService.class);
//  		job.setMapperClass(CountSummaryLogMapper.class);
//  		job.setCombinerClass(IntSumReducer.class);
//  		job.setReducerClass(IntSumReducer.class);
//  		job.setOutputKeyClass(Text.class);
//  		job.setOutputValueClass(IntWritable.class);
//  		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logsummary"));
//  		job.setInputFormatClass(NLinesInputFormat.class);
//  		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logsummary"));
//  			  
//  	    job.waitForCompletion(true);
  	    
  	    job = Job.getInstance(conf, "ProcessLogEvent");
		job.setJarByClass(ProcessLogService.class);
		job.setMapperClass(CountEventLogMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myinput/logevent"));
		job.setInputFormatClass(NLinesInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("/opt/hadoop/hadoop-2.6.0/myoutput/logevent"));
			  
	    job.waitForCompletion(true);
	    
	    System.exit(0);
	}
	
	private static String ModifyInput(String input , String name){
		
		if(input == null)
			return null;
		
		if(input.trim().equals("") || input.trim().equals("null"))
			return null;
		String output = input.replace(name+"=", "");
		if(output.length()>4000){
			output = output.substring(0, 3999);
		}
		return output;	
	}
}