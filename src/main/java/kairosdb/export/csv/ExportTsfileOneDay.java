package kairosdb.export.csv;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ExportTsfileOneDay extends Thread
{
    private Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger LOGGER = LoggerFactory.getLogger(ExportTsfileOneDay.class);
    private long startTime;
    private long endTime;
    private CountDownLatch downLatch;
    private Cluster cluster;
    private List<Metric> metriclist;

    public ExportTsfileOneDay(long startTime, long endTime, CountDownLatch downLatch, Cluster cluster,
                              List<Metric>  metriclist)
    {
        this.startTime = startTime;
        this.endTime = endTime;
        this.downLatch = downLatch;
        this.cluster = cluster;
        this.metriclist = metriclist;
    }

    private void exportDataTable(Map<Long, List<Object>> dataTable, List<String> name, List<String> type, String csvname, String hostname)
    {
        if (!new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + startTime + File.separator + hostname ).exists())
        {
            new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + startTime + File.separator + hostname ).mkdir();
        }

        File file = new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator + startTime + File.separator + hostname + File.separator + csvname);
        LOGGER.info("正在导出 {} 行数据到 {} ...", dataTable.size(), csvname);
        try
        {
            try (FileWriter writer = new FileWriter(file))
            {
                StringBuilder headBuilder = new StringBuilder();
                headBuilder.append("Time");
                for (int i = 0; i < name.size(); i++) {
                    headBuilder.append(",").append(name.get(i)+"\t"+type.get(i));
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
            LOGGER.error("KairosDB数据导出为CSV文件失败", e);
            e.printStackTrace();
        }
        LOGGER.info("导出{}完成", csvname);
    }



    private void exportOneMetricCsv(Metric metric)
    {
        String date = CalculateDate(startTime);
        Session session = cluster.connect();
        Map<Long, List<Object>> dataTable =  new HashMap<>();
        List<String> name = new ArrayList<>();
        List<String> type = new ArrayList<>();
        name.add("secondary_time");
        name.add("tertiary_time");
        type.add("DOUBLE");
        type.add("DOUBLE");
        String cql = "";
        ResultSet resultSet;
        String csvname = "root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric;
        switch (metric.type)
        {
            case "INT"://INT32
                cql = "select * from sagittariuscty.data_int where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + ";\'";
                name.add("value");
                type.add("INT32");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getInt("value"));
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("INT64");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getLong("value"));
                    dataTable.put(row.getTime("primary_time"),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;

            case "FLOAT"://FLOAT
                cql = "select * from sagittariuscty.data_float where host = \'"
                        + metric.host
                        + "\' and time_slice = \'"
                        + date
                        + "\' and metric = \'"
                        + metric.metric
                        + ";\'";
                name.add("value");
                type.add("FLOAT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getFloat("value"));
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("DOUBLE");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getDouble("value"));
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getString("value"));
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    Map<String,String> insmap = row.getMap("value", TypeToken.of(String.class), TypeToken.of(String.class));
                    ObjectMapper mapper = null;
                    try
                    {
                        ins.add(mapper.writeValueAsString(insmap));
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("latitude");
                type.add("DOUBLE");
                name.add("longitude");
                type.add("DOUBLE");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getDouble("latitude"));
                    ins.add(row.getDouble("longitude"));
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    Map<String,String> insmap = row.getMap("value", TypeToken.of(String.class), TypeToken.of(String.class));
                    ObjectMapper mapper = null;
                    try
                    {
                        ins.add(mapper.writeValueAsString(insmap));
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("TEXT");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    Map<String,Double> insmap = row.getMap("value", TypeToken.of(String.class), TypeToken.of(Double.class));
                    ObjectMapper mapper = null;
                    try
                    {
                        ins.add(mapper.writeValueAsString(insmap));
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                    dataTable.put(row.getTime("primary_time"),ins);
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
                        + ";\'";
                name.add("value");
                type.add("BOOLEAN");
                resultSet = session.execute(cql);
                for (Row row : resultSet)
                {
                    List<Object> ins = new ArrayList<>();
                    ins.add(row.getTime("secondary_time"));
                    ins.add(row.getLong("candidate_id"));
                    ins.add(row.getBool("value"));
                    dataTable.put(row.getTime("primary_time"),ins);
                }
                exportDataTable(dataTable,name,type,csvname,metric.host);
                break;
        }
        LOGGER.info(cql);
    }

    @Override
    public void run()
    {
        try
        {
            String host = "";
            for (Metric metric : metriclist)
            {
                host = metric.host;
                exportOneMetricCsv(metric);
            }
            deleteFile(host);
            downLatch.countDown();
        }
        catch (Exception e)
        {
            LOGGER.error("发生异常", e);
        }
    }

    private void deleteFile(String host)
    {
        String tsFilePath = ExportToCsv.dirAbsolutePath + File.separator + Constants.SEQUENCE_DIR + File.separator
                + host + File.separator
                + String.format(Constants.TSFILE_FILE_NAME, startTime, 0, 0);
        TransToTsfile.transToTsfile(
        ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
            + startTime + File.separator + host, tsFilePath);
        if (config.DELETE_CSV)
        {
            File[] files = new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
                    + startTime + File.separator + host).listFiles();
            for (File file : files)
            {
                file.delete();
            }
            new File(ExportToCsv.dirAbsolutePath + File.separator + Constants.CSV_DIR + File.separator
                    + startTime + File.separator + host).delete();
        }
    }

    private String CalculateDate(long time)
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
        return year+"D"+String.valueOf((time-now)/86400000+1);
    }

    private String getUsername()
    {
        return "cty";
    }
}
