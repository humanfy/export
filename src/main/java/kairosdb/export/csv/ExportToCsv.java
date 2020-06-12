package kairosdb.export.csv;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import kairosdb.export.csv.conf.CommandCli;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import kairosdb.export.csv.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.*;
public class ExportToCsv
{
  	private static Config config;
  	private static final Logger LOGGER = LoggerFactory.getLogger(ExportToCsv.class);
  	private static long startTime;
  	private static long endTime;
  	private static int dayNumber;
	private static Cluster cluster;
  	public static void main(String[] args)
	{
		CommandCli cli = new CommandCli();
		if (!cli.init(args))
		{
			System.exit(1);
		}
		config = ConfigDescriptor.getInstance().getConfig();
		if (!new File(config.dirAbsolutePath + File.separator + Constants.CSV_DIR).exists())
		{
	  		new File(config.dirAbsolutePath + File.separator + Constants.CSV_DIR).mkdir();
		}
		if (!config.START_TIME.equals("") && !config.ENDED_TIME.equals("")) {
			startTime = TimeUtils.convertDateStrToTimestamp(config.START_TIME);
			endTime = TimeUtils.convertDateStrToTimestamp(config.ENDED_TIME);
			dayNumber = TimeUtils.timeRange(startTime, endTime);
			CountDownLatch downLatch = new CountDownLatch(dayNumber);
			String[] machines = {"192.168.35.26", "192.168.35.27",
					"192.168.35.28", "192.168.35.29", "192.168.35.30"};
			boolean isConnected = false;
			Session session = null;
			for (int i = 0; i < machines.length; i++) {
				if (isConnected)
					break;
				try {
					cluster = Cluster.builder().addContactPoints(machines[i]).
							withPort(9042).withCredentials("cassandra", "cassandra").build();
					session = cluster.connect();
					isConnected = true;
				} catch (Exception e) {
					continue;
				}
			}
			if (!isConnected) {
				LOGGER.error("Connect to Cassandra failed");
			}

			String cql = "SELECT * from sagittariuscty.metric;";
			ResultSet resultSet = session.execute(cql);
			Map<String, String> typ = new HashMap<>();
			for (Row row : resultSet)
				typ.put(row.getString("metric"), row.getString("value_type"));

			LOGGER.info("metric数量: {}", typ.size());

			Map<String, Integer> v = new HashMap<>();
			int totalhost = 0;
			cql = "SELECT * from sagittariuscty.host;";
			resultSet = session.execute(cql);
			List<String> hosts = new ArrayList<>();
			for (Row row : resultSet) {
				totalhost++;
				hosts.add(row.getString("host"));
				v.put(row.getString("host"), totalhost);
			}

			LOGGER.info("host数量: {}", hosts.size());
			int tot = 0;
			for (String host : hosts) {
				if (host.hashCode() % config.TOTAL_HASH != config.HASH_NUM)
					continue;
				tot++;
			}

			ExecutorService executorService = new ThreadPoolExecutor(config.THREAD_NUM, 1024,
					Long.MAX_VALUE, TimeUnit.SECONDS,
					new LinkedBlockingQueue<>(tot * dayNumber));
			session.close();

			for (long i = 0; i < dayNumber; i++)
			{

			for (String host : hosts) {
				if (host.hashCode() % config.TOTAL_HASH != config.HASH_NUM)
					continue;
				Session session2 = cluster.connect();
				List<Metric> metriclist = new ArrayList();
				cql = "SELECT * from sagittariuscty.latest where host = \'" + host + "\';";
				resultSet = session2.execute(cql);
				for (Row row : resultSet) {
					Metric tmp = new Metric();
					tmp.host = row.getString("host");
					tmp.metric = row.getString("metric");
					tmp.type = typ.get(tmp.metric);
					metriclist.add(tmp);
				}
				LOGGER.info("host {}: 数量 {}", host, metriclist.size());
				executorService.submit(new ExportTsfileOneDay(startTime + i * Constants.TIME_DAY,
						downLatch, cluster, metriclist, host, v.get(host)));

				session2.close();
			}
		}
			executorService.shutdown();
			try
			{
				downLatch.await();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
	  		LOGGER.error("必须指定导出数据的起止时间！");
		}
  	}

}
