package garn.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class CountSerivceLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(CountSerivceLogMapper.class);
	private final static String START = "Start";
	private final static String END = "End";
	private final static String PIPE = "\\|";
	
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		LOG.info("Start ServiceLog "+key);
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
						
						
						String LOGTIME = ProcessLogService.ModifyInput(rowServiceLog[0],"LOGTIME");					
						String SSID = ProcessLogService.ModifyInput(rowServiceLog[5],"SSID");
						String AUDIT_LOG_ID = ProcessLogService.ModifyInput(rowServiceLog[7],"AUDIT_LOG_ID");
						String INVOKE = ProcessLogService.ModifyInput(rowServiceLog[6],"INVOKE");
						String INVOKE_PARENT = "";//ProcessLogService.ModifyInput(rowServiceLog[8],"INVOKE_PARENT");
						String EVENT_LOG_ID = ProcessLogService.ModifyInput(rowServiceLog[8],"EVENT_LOG_ID");
						String EVENT_LOG_NAME =  ProcessLogService.ModifyInput(rowServiceLog[9],"EVENT_LOG_NAME");
						String SERVICE_ID = ProcessLogService.ModifyInput(rowServiceLog[10],"SERVICE_ID");
						String SERVICE_SYSTEM = "";//ProcessLogService.ModifyInput(rowServiceLog[11],"SERVICE_SYSTEM");
						String SERVICE_NAME = "";//ProcessLogService.ModifyInput(rowServiceLog[13],"SERVICE_NAME");
						String REQUEST_MSG = ProcessLogService.ModifyInput(rowServiceLog[11],"REQUEST_MSG");
						String REQUEST_DATE = ProcessLogService.ModifyInput(rowServiceLog[12],"REQUEST_DATE");
						String RESPONSE_CODE = ProcessLogService.ModifyInput(rowServiceLog[13],"RESPONSE_CODE");
						String RESPONSE_MSG = ProcessLogService.ModifyInput(rowServiceLog[14],"RESPONSE_MSG");
						String RESPONSE_DATE = ProcessLogService.ModifyInput(rowServiceLog[15],"RESPONSE_DATE");
						String RESPONSE_FLAG = ProcessLogService.ModifyInput(rowServiceLog[16],"RESPONSE_FLAG");
						String MOBILE_NO = ProcessLogService.ModifyInput(rowServiceLog[17],"MOBILE_NO");
						String NTYPE = ProcessLogService.ModifyInput(rowServiceLog[18],"NTYPE");
						String ADMIN_FLAG = ProcessLogService.ModifyInput(rowServiceLog[19],"ADMIN_FLAG");
						String CHANNEL = ProcessLogService.ModifyInput(rowServiceLog[20],"CHANNEL");
			
						
						String sql = " INSERT INTO GARN_LOG_SERVICE VALUES( GARN_LOG_SERVICE_SEQ.NEXTVAL , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? ,TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF'), ? , ? , TO_TIMESTAMP(?, 'yyyymmdd hh24:mi:ss.FF') , ? , ? , ? , ? , ?) ";
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
