package kairosdb.export.csv;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.session.Session;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class InsertintoIotdb
{
	private static final Logger LOGGER = LoggerFactory.getLogger(TransToTsfile.class);
	public static void insertintoIotdb(String dirPath)
	{
		try
		{
			Session session = new Session("192.168.35.21", 6667, "root", "root");
			session.open();
			if (!new File(dirPath).exists())
			{
				return;
			}
			File[] csvFiles = new File(dirPath).listFiles();
			for (File csvFile : csvFiles)
			{
				LOGGER.info("Name of csvfile: {}", csvFile.getName());
				try (BufferedReader csvReader = new BufferedReader(new FileReader(csvFile)))
				{
					int lines=0;
					try(Stream<String> stream = Files.lines(Paths.get(String.valueOf(csvFile))))
					{
						lines = (int)stream.filter(string->!string.isEmpty()).count() - 1;
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
					LOGGER.info("totalline: {}, filename: {}, dirpath: {}",lines,csvFile.getName(),dirPath);
					Schema schema = new Schema();
					String device = csvFile.getName().replaceAll(".csv","");
					String header = csvReader.readLine();
					String[] sensorFull = Arrays.copyOfRange(
							header.split(","), 1, header.split(",").length);
					ArrayList<String> sensorList = new ArrayList<>(Arrays.asList(sensorFull));
					for (int i = 0; i < sensorList.size(); i++)
					{
						String type = sensorList.get(i).split("\1")[1];
						switch (type)
						{
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
					while ((line = csvReader.readLine()) != null)
					{
						long time = 0;
						try
						{
							time = Long.parseLong(line.split(",")[0]);
						}
						catch (Exception e)
						{
							//LOGGER.error("insert error :{}, Line error : {}", csvFile.getName(),line);
							continue;
						}
						int row = rowBatch.batchSize++;
						timestamps[row] = time;
						String[] points = Arrays.
								copyOfRange(line.split(",", sensorList.size() + 1), 1, sensorList.size()+1);
						for (int i = 0; i < points.length; i++)
						{
							switch (sensorList.get(i).split("\1")[1])
							{
								case "INT32":
									if (points[i].equals("")) points[i]="-1";
									int[] sensor = (int[]) values[i];
									sensor[row] = Integer.parseInt(points[i]);
									break;
								case "INT64":
									if (points[i].equals("")) points[i]="-1";
									long[] sensor2 = (long[]) values[i];
									sensor2[row] = Long.parseLong(points[i]);
									break;
								case "FLOAT":
									if (points[i].equals("")) points[i]="-1";
									float[] sensor3 = (float[]) values[i];
									sensor3[row] = Float.parseFloat(points[i]);
									break;
								case "DOUBLE":
									if (points[i].equals("")) points[i]="-1";
									double[] sensor4 = (double[]) values[i];
									sensor4[row] = Double.parseDouble(points[i]);
									break;
								case "BOOLEAN":
									if (points[i].equals("")) points[i]="false";
									boolean[] sensor5 = (boolean []) values[i];
									sensor5[row] = Boolean.parseBoolean(points[i]);
									break;
								case "TEXT":
									if (points[i].equals("")) points[i]="";
									Binary[] sensor6 = (Binary []) values[i];
									sensor6[row] =  Binary.valueOf(points[i]);
									break;
							}
						}
					}
					for (int i = 0; i < 5; i++)
					{
						try
						{
							session.insertBatch(rowBatch);
							break;
						}
						catch (Exception e)
						{
							if (i < 4) continue;
							else throw e;
						}
					}

				}
				catch (Exception e)
				{
					LOGGER.error("insert error: {}", csvFile.getName());
					e.printStackTrace();
				}
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
