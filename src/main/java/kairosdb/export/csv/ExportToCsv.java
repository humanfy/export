package kairosdb.export.csv;

import java.io.File;
import java.io.IOException;
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
  	public static String dirAbsolutePath;
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
		if (!new File(dirAbsolutePath + File.separator + Constants.CSV_DIR).exists())
		{
	  		new File(dirAbsolutePath + File.separator + Constants.CSV_DIR).mkdir();
		}
		if (!config.START_TIME.equals("") && !config.ENDED_TIME.equals(""))
		{
	  		startTime = TimeUtils.convertDateStrToTimestamp(config.START_TIME);
	  		endTime = TimeUtils.convertDateStrToTimestamp(config.ENDED_TIME);
	  		dayNumber = TimeUtils.timeRange(startTime, endTime);
	  		CountDownLatch downLatch = new CountDownLatch(dayNumber);
	  		cluster = Cluster.builder().addContactPoints("127.0.0.1").
					withPort(9042).withCredentials("cassandra", "cassandra").build();
	  		Session session = cluster.connect();

	  		String cql = "SELECT * from sagittariuscty.metric;";
			ResultSet resultSet = session.execute(cql);
			Map<String,String> typ = new HashMap<>();
			for (Row row : resultSet)
				typ.put(row.getString("metric"),row.getString("value_type"));

			cql = "SELECT * from sagittariuscty.host;";
			resultSet = session.execute(cql);
			List<String> hosts = new ArrayList<>();
			for (Row row : resultSet)
				hosts.add(row.getString("host"));

			for (String host : hosts)
			{
				List<Metric>  metriclist = new ArrayList();
				cql = "SELECT * from sagittariuscty.latest where host = \'"+host+"\';";
				resultSet = session.execute(cql);
				for (Row row : resultSet)
				{
					Metric tmp = new Metric();
					tmp.host = row.getString("host");
					tmp.metric = row.getString("metric");
					tmp.type = typ.get(tmp.metric);
					metriclist.add(tmp);
				}

				ExecutorService executorService = new ThreadPoolExecutor(config.THREAD_NUM, 1024,
						Long.MAX_VALUE, TimeUnit.SECONDS,
						new LinkedBlockingQueue<>(4096));
				for (long i = 0; i < dayNumber; i++)
				{
					if (i == dayNumber - 1)
					{
						executorService.submit(
								new ExportTsfileOneDay(startTime + i * Constants.TIME_DAY,
										endTime, downLatch, cluster, metriclist));
					}
					else
					{
						executorService.submit(
								new ExportTsfileOneDay(startTime + i * Constants.TIME_DAY,
										startTime + (i + 1) * Constants.TIME_DAY,  downLatch, cluster, metriclist));
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
		}
		else
		{
	  		LOGGER.error("必须指定导出数据的起止时间！");
		}
  	}

}
