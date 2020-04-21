package kairosdb.export.csv;

import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.*;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.session.Session;
import org.apache.thrift.TException;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class InsertintoIotdb {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransToTsfile.class);
	public static void insertintoIotdb(String dirPath)
	{
		try
		{
			Session session = new Session("127.0.0.1", 6667, "root", "root");
			session.open();
			try
			{
				if (!new File(dirPath).exists())
				{
					return;
				}
				File[] csvFiles = new File(dirPath).listFiles();
				for (File csvFile : csvFiles)
				{
					Map<String,Boolean> tsFileMeasurement = new HashMap<>();
					LOGGER.info("Name of csvfile: {}", csvFile.getName());
					try (BufferedReader csvReader = new BufferedReader(new FileReader(csvFile)))
					{
						int lines=0;
						try(Stream<String> stream = Files.lines(Paths.get(String.valueOf(csvFile)))){

							lines = (int)stream.count();

						} catch (IOException e)
						{
							e.printStackTrace();
						}

						Schema schema = new Schema();
						String device = csvFile.getName().replaceAll(".csv","");
						String header = csvReader.readLine();
						String[] sensorFull = Arrays.copyOfRange(
								header.split(","), 1, header.split(",").length);
						ArrayList<String> sensorList = new ArrayList<>(Arrays.asList(sensorFull));
						for (int i = 0; i < sensorList.size(); i++)
						{
							String type = sensorList.get(i).split("\t")[1];
							switch (type)
							{
								case "INT32":
									schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\t")[0], TSDataType.INT32, TSEncoding.RLE));
									break;
								case "INT64":
									schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\t")[0], TSDataType.INT64, TSEncoding.RLE));
									break;
								case "FLOAT":
									schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\t")[0], TSDataType.FLOAT, TSEncoding.GORILLA));
									break;
								case "DOUBLE":
									schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\t")[0], TSDataType.DOUBLE, TSEncoding.GORILLA));
									break;
								case "TEXT":
									schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\t")[0], TSDataType.TEXT, TSEncoding.PLAIN));
									break;
								case "BOOLEAN":
									schema.registerMeasurement(new MeasurementSchema(sensorList.get(i).split("\t")[0], TSDataType.BOOLEAN, TSEncoding.PLAIN));
									break;
							}
							sensorList.set(i, sensorList.get(i).split("\t")[0]);
						}
						RowBatch rowBatch = schema.createRowBatch(device, lines);
						String line;
						long[] timestamps = rowBatch.timestamps;
						Object[] values = rowBatch.values;
						while ((line = csvReader.readLine()) != null)
						{
							long time = Long.parseLong(line.split(",")[0]);
							int row = rowBatch.batchSize++;
							timestamps[row] = time;
							String[] points = Arrays.
									copyOfRange(line.split(",", sensorList.size() + 1), 1, sensorList.size());

							for (int i = 0; i < points.length; i++)
							{
								Object[] sensor = (Object[]) values[i];
								if (points[i].equals(""))
									continue;
								switch (sensorList.get(i).split("\t")[1])
								{
									case "INT32":
										sensor[row] = Integer.parseInt(points[i]);
										break;
									case "INT64":
										sensor[row] = Long.parseLong(points[i]);
										break;
									case "FLOAT":
										sensor[row] = Float.parseFloat(points[i]);
										break;
									case "DOUBLE":
										sensor[row] = Double.parseDouble(points[i]);
										break;
									case "BOOLEAN":
										sensor[row] = Boolean.parseBoolean(points[i]);
										break;
									case "TEXT":
										sensor[row] =  Binary.valueOf(points[i]);
										break;
								}
							}
						}
						session.insertBatch(rowBatch);
						rowBatch.reset();
					}
				}
			}
			catch(Exception e)
			{
				LOGGER.error("insert error", e);
			}
			session.close();
		}
		catch (Exception e)
		{
			LOGGER.error("error occurs when writing to iotdb", e);
		}
	}

}
