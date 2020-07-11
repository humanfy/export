package kairosdb.export.csv;


import kairosdb.export.csv.conf.Config;
import kairosdb.export.csv.conf.ConfigDescriptor;
import kairosdb.export.csv.conf.Constants;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.*;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.session.Session;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import static kairosdb.export.csv.ExportToCsv.metriclists;
import static kairosdb.export.csv.ExportTsfileOneDay.getUsername;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class InsertintoIotdb {

	private static Config config = ConfigDescriptor.getInstance().getConfig();
	private static final Logger LOGGER = LoggerFactory.getLogger(TransToTsfile.class);

	public static void insertintoIotdb(String dirPath) {
		try {
			Session session = new Session(config.IOTDB_IP.split(",")[config.HASH_NUM], 6667, "root", "root");
			session.open();
			if (!new File(dirPath).exists()) {
				return;
			}
			File[] csvFiles = new File(dirPath).listFiles();
			for (File csvFile : csvFiles) {
				LOGGER.info("Name of csvfile: {}", csvFile.getName());
				try (BufferedReader csvReader = new BufferedReader(new FileReader(csvFile))) {
					int lines = 0;
					try (Stream<String> stream = Files.lines(Paths.get(String.valueOf(csvFile)))) {
						lines = (int) stream.filter(string -> !string.isEmpty()).count() - 1;
					} catch (IOException e) {
						e.printStackTrace();
					}
					LOGGER.info("totalline: {}, filename: {}, dirpath: {}", lines, csvFile.getName(), dirPath);
					Schema schema = new Schema();
					String device = csvFile.getName().replaceAll(".csv", "");
					String header = csvReader.readLine();
					String[] sensorFull = Arrays.copyOfRange(
							header.split(","), 1, header.split(",").length);
					ArrayList<String> sensorList = new ArrayList<>(Arrays.asList(sensorFull));
					for (int i = 0; i < sensorList.size(); i++) {
						String type = sensorList.get(i).split("\1")[1];
						switch (type) {
							case "INT32":
								schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\1")[0], TSDataType.INT32, TSEncoding.RLE));
								break;
							case "INT64":
								schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\1")[0], TSDataType.INT64, TSEncoding.RLE));
								break;
							case "FLOAT":
								schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\1")[0], TSDataType.FLOAT, TSEncoding.GORILLA));
								break;
							case "DOUBLE":
								schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\1")[0], TSDataType.DOUBLE, TSEncoding.GORILLA));
								break;
							case "TEXT":
								schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\1")[0], TSDataType.TEXT, TSEncoding.PLAIN));
								break;
							case "BOOLEAN":
								schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\1")[0], TSDataType.BOOLEAN, TSEncoding.PLAIN));
								break;
						}
					}
					RowBatch rowBatch = schema.createRowBatch(device, lines);
					String line;
					long[] timestamps = rowBatch.timestamps;
					Object[] values = rowBatch.values;
					while ((line = csvReader.readLine()) != null) {
						long time = 0;
						try {
							time = Long.parseLong(line.split(",")[0]);
						} catch (Exception e) {
							//LOGGER.error("insert error :{}, Line error : {}", csvFile.getName(),line);
							continue;
						}
						int row = rowBatch.batchSize++;
						timestamps[row] = time;
						String[] points = Arrays.
								copyOfRange(line.split(",", sensorList.size() + 1), 1, sensorList.size() + 1);
						for (int i = 0; i < points.length; i++) {
							switch (sensorList.get(i).split("\1")[1]) {
								case "INT32":
									if (points[i].equals("")) points[i] = "-1";
									int[] sensor = (int[]) values[i];
									sensor[row] = Integer.parseInt(points[i]);
									break;
								case "INT64":
									if (points[i].equals("")) points[i] = "-1";
									long[] sensor2 = (long[]) values[i];
									sensor2[row] = Long.parseLong(points[i]);
									break;
								case "FLOAT":
									if (points[i].equals("")) points[i] = "-1";
									float[] sensor3 = (float[]) values[i];
									sensor3[row] = Float.parseFloat(points[i]);
									break;
								case "DOUBLE":
									if (points[i].equals("")) points[i] = "-1";
									double[] sensor4 = (double[]) values[i];
									sensor4[row] = Double.parseDouble(points[i]);
									break;
								case "BOOLEAN":
									if (points[i].equals("")) points[i] = "false";
									boolean[] sensor5 = (boolean[]) values[i];
									sensor5[row] = Boolean.parseBoolean(points[i]);
									break;
								case "TEXT":
									if (points[i].equals("")) points[i] = "";
									Binary[] sensor6 = (Binary[]) values[i];
									sensor6[row] = Binary.valueOf(points[i]);
									break;
							}
						}
					}
					for (int i = 0; i < 3; i++) {
						try {
							session.insertBatch(rowBatch);
							break;
						} catch (Exception e) {
							if (i < 4) continue;
							else {
								LOGGER.error("insertbatch error: {}", csvFile.getName());
								e.printStackTrace();
							}
						}
					}

				} catch (Exception e) {
					LOGGER.error("insert error: {}", csvFile.getName());
					e.printStackTrace();
				}
			}
			session.close();
		} catch (Exception e) {
			LOGGER.error("error occurs when writing to iotdb", e);
			e.printStackTrace();
		}
	}

	public static void loadintoIotdb(long startTime)
	{
		File tmpPath = new File(config.tmpPath);
		List<fileinfo> files = new ArrayList<>();
		for (File f : tmpPath.listFiles())
		{
			fileinfo tmp = new fileinfo();
			if (f.isDirectory())
			{
				tmp.f = new File(f.getAbsolutePath() + File.separator + startTime + "-0-0.tsfile");
				tmp.host = f.getName();
				files.add(tmp);
			}
		}
		try
		{
			File f = new File(config.tmpPath + File.separator + startTime + "-0-0.tsfile");
			if (!f.getParentFile().exists())
			{
				f.getParentFile().mkdirs();
			}
			try (TsFileWriter tsFileWriter = new TsFileWriter(f))
			{
				Map<String, Boolean> tsFileMeasurement = new HashMap<>();
				for (fileinfo tsfile : files)
				{
					String path = tsfile.f.getAbsolutePath();
					TsFileSequenceReader reader = new TsFileSequenceReader(path);
					ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
					List<Metric> metrics = metriclists.get(tsfile.host);
					for (Metric metric : metrics)
					{
						if (metric.type == null) continue;
						ArrayList<Path> paths = new ArrayList<>();
						paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"secondary_time"));
						paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"tertiary_time"));
						switch(metric.type)
						{
							case "INT":
							case "LONG":
								paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"valueInt64"));
								break;
							case "FLOAT":
							case "DOUBLE":
								paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"valueDouble"));
								break;
							case "STRING":
							case "MAP":
							case "MAL":
							case "GPS":
								paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"valueText"));
								break;
							case "GEO":
								paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"latitude"));
								paths.add(new Path("root." + getUsername() + "." + metric.host + "." + metric.host + "." + metric.metric ,"longitude"));
								break;
						}
						QueryExpression queryExpression = QueryExpression.create(paths, null);
						QueryDataSet queryDataSet = readTsFile.query(queryExpression);
						if (!queryDataSet.hasNext()) continue;
						if (!tsFileMeasurement.containsKey(metric.metric+"_secondary_time"))
						{
							tsFileMeasurement.put(metric.metric+"_secondary_time",true);
							tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_secondary_time",
									TSDataType.DOUBLE, TSEncoding.GORILLA));
						}
						if (!tsFileMeasurement.containsKey(metric.metric+"_tertiary_time"))
						{
							tsFileMeasurement.put(metric.metric+"_tertiary_time",true);
							tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_tertiary_time",
									TSDataType.DOUBLE, TSEncoding.GORILLA));
						}
						switch (metric.type)
						{
							case "INT":
							case "LONG":
								tsFileMeasurement.put(metric.metric+"_valueInt64",true);
								tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_valueInt64",
										TSDataType.INT64, TSEncoding.RLE));
								break;
							case "FLOAT":
							case "DOUBLE":
								tsFileMeasurement.put(metric.metric+"_valueDouble",true);
								tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_valueDouble",
										TSDataType.DOUBLE, TSEncoding.GORILLA));
								break;
							case "STRING":
							case "MAP":
							case "MAL":
							case "GPS":
								tsFileMeasurement.put(metric.metric+"_valueText",true);
								tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_valueText",
										TSDataType.TEXT, TSEncoding.PLAIN));
								break;
							case "GEO":
								tsFileMeasurement.put(metric.metric+"_latitude",true);
								tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_latitude",
										TSDataType.DOUBLE, TSEncoding.GORILLA));
								tsFileMeasurement.put(metric.metric+"_longitude",true);
								tsFileWriter.addMeasurement(new MeasurementSchema(metric.metric+"_longitude",
										TSDataType.DOUBLE, TSEncoding.GORILLA));
								break;
						}
						while (queryDataSet.hasNext())
						{
							RowRecord now = queryDataSet.next();
							TSRecord tsRecord = new TSRecord(now.getTimestamp(), "root." + getUsername() + "." + metric.host + "." + metric.host);
							int len = now.getFields().size();
							for (int i=0; i<len; i++)
							{
								DataPoint Point;
								switch (queryDataSet.getDataTypes().get(i))
								{
									case INT32:
										Point = new IntDataPoint(queryDataSet.getPaths().get(i).getMeasurement(), now.getFields().get(i).getIntV());
										tsRecord.addTuple(Point);
										break;
									case INT64:
										Point = new LongDataPoint(queryDataSet.getPaths().get(i).getMeasurement(), now.getFields().get(i).getLongV());
										tsRecord.addTuple(Point);
										break;
									case FLOAT:
										Point = new FloatDataPoint(queryDataSet.getPaths().get(i).getMeasurement(), now.getFields().get(i).getFloatV());
										tsRecord.addTuple(Point);
										break;
									case DOUBLE:
										Point = new DoubleDataPoint(queryDataSet.getPaths().get(i).getMeasurement(), now.getFields().get(i).getDoubleV());
										tsRecord.addTuple(Point);
										break;
									case BOOLEAN:
										Point = new BooleanDataPoint(queryDataSet.getPaths().get(i).getMeasurement(), now.getFields().get(i).getBoolV());
										tsRecord.addTuple(Point);
										break;
									case TEXT:
										Point = new StringDataPoint(queryDataSet.getPaths().get(i).getMeasurement(), now.getFields().get(i).getBinaryV());
										tsRecord.addTuple(Point);
										break;
								}
							}
							try
							{
								tsFileWriter.write(tsRecord);
							}
							catch (Exception e)
							{
								LOGGER.error("write record error: {}, error file: {}, record: {}", e.getMessage(),tsfile.f.getAbsolutePath(), now.toString());
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			LOGGER.error("error occurs when generate tsfile", e);
			e.printStackTrace();
		}
		long LIMIT = 64;
		try
		{
			Session session = new Session(config.IOTDB_IP.split(",")[config.HASH_NUM], 6667, "root", "root");
			session.open();
			try
			{
				File f = new File(config.tmpPath + File.separator + startTime + "-0-0.tsfile");
				if (f.length()>=LIMIT)
					session.executeNonQueryStatement("load " + f.getAbsolutePath());
			}
			catch (Exception e)
			{
				LOGGER.error("error occurs when load to iotdb", e);
				e.printStackTrace();
			}
			session.close();
		}
		catch (Exception e)
		{
			LOGGER.error("error occurs when connect to iotdb", e);
			e.printStackTrace();
		}
		if (config.DELETE_CSV)
		{
			for (fileinfo file : files)
			{
				file.f.delete();
			}
		}
	}

	static class fileinfo
	{
		File f;
		String host;
	}

	public static void oldloadintoIotdb(String filePath)
	{
		long LIMIT = 64;
		try
		{
			Session session = new Session(config.IOTDB_IP.split(",")[config.HASH_NUM], 6667, "root", "root");
			session.open();
			try
			{
				File f = new File(filePath);
				if (f.length()>=LIMIT)
					session.executeNonQueryStatement("load " + filePath);
			}
			catch (Exception e)
			{

			}
			session.close();
		}
		catch (Exception e)
		{
			LOGGER.error("error occurs when writing to iotdb", e);
			e.printStackTrace();
		}
	}
}
