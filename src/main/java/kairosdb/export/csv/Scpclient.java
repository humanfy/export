package kairosdb.export.csv;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Scpclient {
	private static final Logger LOGGER = LoggerFactory.getLogger(ExportToCsv.class);
	static private Scpclient instance;

	static synchronized public Scpclient getInstance(String IP, int port,
													 String username, String passward) {
		if (instance == null) {
			instance = new Scpclient(IP, port, username, passward);
		}
		return instance;
	}

	public Scpclient(String IP, int port, String username, String passward) {
		this.ip = IP;
		this.port = port;
		this.username = username;
		this.password = passward;
	}

	/**
	 * 远程拷贝文件
	 * @param remoteFile  远程源文件路径
	 * @param localTargetDirectory 本地存放文件路径
	 */
	public void getFile(String remoteFile, String localTargetDirectory) {
		Connection conn = new Connection(ip,port);
		try {
			conn.connect();
			boolean isAuthenticated = conn.authenticateWithPassword(username,
					password);
			if (isAuthenticated == false) {
				LOGGER.error("authentication failed");
			}
			SCPClient client = new SCPClient(conn);
			client.get(remoteFile, localTargetDirectory);
			conn.close();
		} catch ( IOException e) {
			LOGGER.error("Remotefile: {},Errormsg: {} ",remoteFile, e.getMessage());
		}
	}

	private String ip;
	private int port;
	private String username;
	private String password;

}

