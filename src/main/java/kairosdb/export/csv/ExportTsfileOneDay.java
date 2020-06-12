package kairosdb.export.csv;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.reflect.TypeToken;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class ExportTsfileOneDay extends Thread
{
    private Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportTsfileOneDay.class);
    private long startTime;
    private CountDownLatch downLatch;
    private Cluster cluster;
    private List<Metric> metriclist;
    private String host;
    private int hostNum;
    private List<String> timeStr = new ArrayList<>();
    private List<String> IPStr = new ArrayList<>();
    private List<String> PathStr = new ArrayList<>();

    public ExportTsfileOneDay(long startTime, CountDownLatch downLatch, Cluster cluster,
                              List<Metric>  metriclist, String host, int hostNum)
    {
        this.startTime = startTime;
        this.downLatch = downLatch;
        this.cluster = cluster;
        this.metriclist = metriclist;
        this.host = host;
        this.hostNum = hostNum;
        timeStr.add("2019D182");
        timeStr.add("2019D197");
        timeStr.add("2019D213");
        timeStr.add("2019D228");
        timeStr.add("2019D244");
        timeStr.add("2019D259");
        timeStr.add("2019D274");
        timeStr.add("2019D289");
        timeStr.add("2019D305");
        timeStr.add("2019D320");
        timeStr.add("2019D335");
        timeStr.add("2019D350");
        timeStr.add("2020D001");
        timeStr.add("2020D016");
        timeStr.add("2020D032");
        timeStr.add("2020D047");
        timeStr.add("2020D061");
        timeStr.add("2020D076");
        timeStr.add("2020D092");
        timeStr.add("2020D107");

        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.22");
        IPStr.add("192.168.35.44");
        IPStr.add("192.168.35.44");
        IPStr.add("192.168.35.44");
        IPStr.add("192.168.35.44");

        PathStr.add("/data1/20190701/fy/export/res/sequence");
        PathStr.add("/data2/20190716/fy/export/res/sequence");
        PathStr.add("/data3/20190801/fy/export/res/sequence");
        PathStr.add("/data4/20190816/fy/export/res/sequence");
        PathStr.add("/data5/20190901/fy/export/res/sequence");
        PathStr.add("/data6/20190916/fy/export/res/sequence");
        PathStr.add("/data1/20191001/fy/export/res/sequence");
        PathStr.add("/data2/20191016/fy/export/res/sequence");
        PathStr.add("/data3/20191101/fy/export/res/sequence");
        PathStr.add("/data4/20191116/fy/export/res/sequence");
        PathStr.add("/data5/20191201/fy/export/res/sequence");
        PathStr.add("/data6/20191216/fy/export/res/sequence");
        PathStr.add("/data1/20200101/fy/export/res/sequence");
        PathStr.add("/data2/20200116/fy/export/res/sequence");
        PathStr.add("/data3/20200201/fy/export/res/sequence");
        PathStr.add("/data4/20200216/fy/export/res/sequence");
        PathStr.add("/data1/20200101/fy/export/res/sequence");
        PathStr.add("/data2/20200116/fy/export/res/sequence");
        PathStr.add("/data3/20200201/fy/export/res/sequence");
        PathStr.add("/data4/20200216/fy/export/res/sequence");

    }

    private void merge()
    {
        Scpclient scpclient = Scpclient.getInstance(getIPfromtime(startTime),22,"root", "tykj@2018");
        File file = new File(config.tmpPath + File.separator + host);
        if (!file.exists())
        {
            file.mkdirs();
        }
        scpclient.getFile(getPathfromtime(startTime) + File.separator + host + File.separator + startTime +"-0-0.tsfile",config.tmpPath + File.separator + host);
        File file2 = new File(config.tmpPath + File.separator + host + File.separator + startTime + "-0-0.tsfile");
        file2.renameTo(new File(config.tmpPath + File.separator + host + File.separator + (startTime + hostNum) + "-0-0.tsfile"));
        InsertintoIotdb.loadintoIotdb(config.tmpPath + File.separator + host + File.separator + (startTime+hostNum) + "-0-0.tsfile");
        if (config.DELETE_CSV)
        {
            File file3 = new File(config.tmpPath + File.separator + host + File.separator + (startTime + hostNum) + "-0-0.tsfile");
            file3.delete();
        }
    }

    private void exportDataTable(Map<Long, List<Object>> dataTable, List<String> name, List<String> type, String csvname, String hostname)
    {
        File file = new File(config.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + startTime + File.separator + hostname + File.separator + csvname);

        if (!file.getParentFile().exists())
        {
            file.getParentFile().mkdirs();
        }
        dataTable = sortMapByKey(dataTable);
        LOGGER.info("正在导出 {} 行数据到 {} ...", dataTable.size(), csvname);
        try
        {
            try (FileWriter writer = new FileWriter(file))
            {
                StringBuilder headBuilder = new StringBuilder();
                headBuilder.append("Time");
                for (int i = 0; i < name.size(); i++) {
                    headBuilder.append(",").append(name.get(i)+"\1"+type.get(i));
                }
                headBuilder.append("\n");
                writer.write(headBuilder.toString());
                for (Map.Entry<Long, List<Object>> entry : dataTable.entrySet())
                {
                    StringBuilder lineBuilder = new StringBuilder(entry.getKey() + "");
                    List<Object> record = entry.getValue();
                    for (Object value : record)
                    {
                        if (value != null)
                        {
                            lineBuilder.append(",").append(value);
                        }
                        else
                        {
                            lineBuilder.append(",");
                        }
                    }
                    lineBuilder.append("\n");
                    writer.write(lineBuilder.toString());
                }
            }
        }
        catch (IOException e)
        {
            LOGGER.error("数据导出为CSV文件失败", e);
            e.printStackTrace();
        }
        LOGGER.info("导出{}完成", csvname);
    }



    private void exportOneMetricCsv(Metric metric)
    {
        String date = CalculateDate(startTime,false);
        Session session = cluster.connect();
        Map<Long, List<Object>> dataTable =  new TreeMap<>();
        List<String> name = new ArrayList<>();
        List<String> type = new ArrayList<>();
        name.add("secondary_time");
        name.add("tertiary_time");
        type.add("DOUBLE");
        type.add("DOUBLE");
        String cql = "";

        ResultSet resultSet;
        String csvname = "root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric + ".csv";
        LOGGER.info("{}, {}, {}",metric.host,metric.metric,metric.type);
        if (metric.type==null)
        {
            LOGGER.info("没有找到 {} 工况的类型", metric.metric);
            session.close();
            return;
        }
        switch (metric.type)
        {
            case "INT"://INT64
                cql = "select * from sagittariuscty.data_int where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueInt64");
                type.add("INT64");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add((long)row.getInt("value"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "LONG"://INT64
                cql = "select * from sagittariuscty.data_long where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueInt64");
                type.add("INT64");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getLong("value"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "FLOAT"://DOUBLE
                cql = "select * from sagittariuscty.data_float where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueDouble");
                type.add("DOUBLE");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add((double)row.getFloat("value"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "DOUBLE"://DOUBLE
                cql = "select * from sagittariuscty.data_double where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueDouble");
                type.add("DOUBLE");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getDouble("value"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "STRING"://TEXT
                cql = "select * from sagittariuscty.data_text where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueText");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getString("value"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "MAP"://map<TEXT,TEXT>
                cql = "select * from sagittariuscty.data_map where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueText");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    Map<String,String> insmap = row.getMap("value", TypeToken.of(String.class), TypeToken.of(String.class));
                    ObjectMapper mapper = new ObjectMapper();
                    try
                    {
                        ins.add(mapper.writeValueAsString(insmap));
                    }
                    catch (IOException e)
                    {
                        LOGGER.error("Map 类型转换发生异常 ： {}, cql : {}", insmap,cql );
                        e.printStackTrace();
                    }
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "GEO"://DOUBLE,DOUBLE
                cql = "select * from sagittariuscty.data_geo where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("latitude");
                type.add("DOUBLE");
                name.add("longitude");
                type.add("DOUBLE");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getDouble("latitude"));
                    ins.add(row.getDouble("longitude"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "MAL"://map<TEXT,TEXT>
                cql = "select * from sagittariuscty.data_mal where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueText");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    Map<String,String> insmap = row.getMap("value", TypeToken.of(String.class), TypeToken.of(String.class));
                    ObjectMapper mapper = new ObjectMapper();
                    try
                    {
                        ins.add(mapper.writeValueAsString(insmap));
                    }
                    catch (IOException e)
                    {
                        LOGGER.error("Map 类型转换发生异常 ： {}, cql : {}", insmap,cql );
                        e.printStackTrace();
                    }
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "GPS"://map<TEXT,DOUBLE>
                cql = "select * from sagittariuscty.data_gps where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("valueText");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    Map<String,Double> insmap = row.getMap("value", TypeToken.of(String.class), TypeToken.of(Double.class));
                    ObjectMapper mapper = new ObjectMapper();
                    try
                    {
                        ins.add(mapper.writeValueAsString(insmap));
                    }
                    catch (IOException e)
                    {
                        LOGGER.error("Map 类型转换发生异常 ： {}, cql : {}", insmap,cql );
                        e.printStackTrace();
                    }
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "BOOLEAN"://BOOLEAN
                cql = "select * from sagittariuscty.data_boolean where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + "\';";
                name.add("value_BOOLEAN");
                type.add("BOOLEAN");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    if (row.getTimestamp("secondary_time")==null)
                        ins.add(-1);
                    else
                        ins.add(row.getTimestamp("secondary_time").getTime());
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getBool("value"));
                    dataTable.put(row.getTimestamp("primary_time").getTime(),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;
        }
        LOGGER.info("查询cql语句 {}",cql);
        session.close();
    }

    @Override
    public void run()
    {
        try
        {
            if (config.IS_MERGE)
            {
                if (metriclist.size()!=0)
                    merge();
            }
            else
            {
                for (Metric metric : metriclist)
                {
                    exportOneMetricCsv(metric);
                }
                deleteFile(host);
            }
        }
        catch (Exception e)
        {
            LOGGER.error("发生异常", e);
            e.printStackTrace();
        }
        finally
        {
            LOGGER.info("Host :{} date:{} is done",host,CalculateDate(startTime,false));
            downLatch.countDown();
        }
    }

    private void deleteFile(String host)
    {
    	if (config.INSERTINTOIOTDB)
		{
			InsertintoIotdb.insertintoIotdb(config.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
					+ startTime + File.separator + host);
		}
    	else
		{
			String tsFilePath = config.dirAbsolutePath + File.separator + Constants.SEQUENCE_DIR + File.separator
					+ host + File.separator
					+ String.format(Constants.TSFILE_FILE_NAME, startTime, 0, 0);
			TransToTsfile.transToTsfile(
                    config.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
							+ startTime + File.separator + host, tsFilePath);
		}
        if (config.DELETE_CSV)
        {
            File[] files = new File(config.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
                    + startTime + File.separator + host).listFiles();
            for (File file : files)
            {
                file.delete();
            }
            new File(config.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
                    + startTime + File.separator + host).delete();
        }
    }

    private String CalculateDate(long time,Boolean completion)
    {
        int year = 2020;
        String str = year+"-01-01 00:00:00";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = null;
        try
        {
            date = simpleDateFormat.parse(str);
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }
        long now = date.getTime();
        while (time<now)
        {
            year -= 1;
            str = year+"-01-01 00:00:00";
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try
            {
                date = simpleDateFormat.parse(str);
            }
            catch (ParseException e)
            {
                e.printStackTrace();
            }
            now = date.getTime();
        }
        if (completion)
        {
            if (((time - now) / Constants.TIME_DAY + 1) < 10)
                return year + "D00" + ((time - now) / Constants.TIME_DAY + 1);
            else if (((time - now) / Constants.TIME_DAY + 1) < 100)
                return year + "D0" + ((time - now) / Constants.TIME_DAY + 1);
            else return year + "D" + ((time - now) / Constants.TIME_DAY + 1);
        }
        else return year + "D" + ((time - now) / Constants.TIME_DAY + 1);
    }

    private String getUsername()
    {
        return "CTY";
    }

    private String getIPfromtime(long time)
    {
        String str = CalculateDate(time,true);
        String ret = null;
        for (int i=0; i<timeStr.size(); i++)
        {
            if (str.compareTo(timeStr.get(i)) >= 0)
                ret = IPStr.get(i);
        }
        return ret;
    }

    private String getPathfromtime(long time)
    {

        String str = CalculateDate(time,true);
        String ret = null;
        for (int i=0; i<timeStr.size(); i++)
        {
            if (str.compareTo(timeStr.get(i)) >= 0)
                ret = PathStr.get(i);
        }
        return ret;
    }

    public static Map<Long, List<Object>> sortMapByKey(Map<Long, List<Object>> map)
    {
        if (map == null)
        {
            return null;
        }
        Map<Long, List<Object>> sortMap = new TreeMap<Long, List<Object>>(new MapKeyComparator());
        sortMap.putAll(map);
        return sortMap;
    }


    static class MapKeyComparator implements Comparator<Long>
    {
        @Override
        public int compare(Long a1, Long a2)
        {
            if (a1<a2) return -1;
            else return 1;
        }
    }
}


