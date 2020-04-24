package kairosdb.export.csv;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.*;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransToTsfile
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransToTsfile.class);
  	public static void transToTsfile(String dirPath, String tsPath)
  	{
  		try
		{
      		File f = new File(tsPath);
      		if (!f.getParentFile().exists())
      		{
        		f.getParentFile().mkdirs();
      		}
      		try (TsFileWriter tsFileWriter = new TsFileWriter(f))
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
            			String header = csvReader.readLine();
            			String[] sensorFull = Arrays.copyOfRange(
            					header.split(","), 1, header.split(",").length);
            			ArrayList<String> sensorList = new ArrayList<>(Arrays.asList(sensorFull));
            			String device = csvFile.getName().replaceAll(".csv","");
						List<TSDataType> tsDataTypes = Arrays.asList(new TSDataType[sensorList.size()]);
            			for (int i = 0; i < sensorList.size(); i++)
            			{
            				String type = sensorList.get(i).split("\t")[1];
            				switch (type)
							{
								case "INT32":
									tsDataTypes.set(i, TSDataType.INT32);
									break;
								case "INT64":
									tsDataTypes.set(i, TSDataType.INT64);
									break;
								case "FLOAT":
									tsDataTypes.set(i, TSDataType.FLOAT);
									break;
								case "DOUBLE":
									tsDataTypes.set(i, TSDataType.DOUBLE);
									break;
								case "TEXT":
									tsDataTypes.set(i, TSDataType.TEXT);
									break;
								case "BOOLEAN":
									tsDataTypes.set(i, TSDataType.BOOLEAN);
									break;
							}
              				sensorList.set(i, sensorList.get(i).split("\t")[0]);
            			}
            			String line;
            			while ((line = csvReader.readLine()) != null)
            			{
              				long time = Long.parseLong(line.split(",")[0]);
              				TSRecord tsRecord = new TSRecord(time, device);
              				String[] points = Arrays.
									copyOfRange(line.split(",", sensorList.size() + 1), 1, sensorList.size()+1);
             			 	for (int i = 0; i < points.length; i++)
             			 	{
                				if (!tsFileMeasurement.containsKey(sensorList.get(i)))
                				{
									tsFileMeasurement.put(sensorList.get(i),true);
									switch (tsDataTypes.get(i))
									{
										case INT32:
										case INT64:
											try
											{
												tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
														tsDataTypes.get(i), TSEncoding.RLE));
											}
											catch (Exception e)
											{
												e.printStackTrace();
											}
											break;
										case FLOAT:
										case DOUBLE:
											try
											{
												tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
														tsDataTypes.get(i), TSEncoding.GORILLA));
											}
											catch (Exception e)
											{
												e.printStackTrace();
											}
											break;
										case BOOLEAN:
										case TEXT:
											try
											{
												tsFileWriter.addMeasurement(new MeasurementSchema(sensorList.get(i),
														tsDataTypes.get(i), TSEncoding.PLAIN));
											}
											catch (Exception e)
											{
												e.printStackTrace();
											}
											break;
									}
								}
								DataPoint Point;
								if (points[i].equals(""))
									continue;
								switch (tsDataTypes.get(i)) {
									case INT32:
										Point = new IntDataPoint(sensorList.get(i), Integer.parseInt(points[i]));
										tsRecord.addTuple(Point);
										break;
									case INT64:
										Point = new LongDataPoint(sensorList.get(i), Long.parseLong(points[i]));
										tsRecord.addTuple(Point);
										break;
									case FLOAT:
										Point = new FloatDataPoint(sensorList.get(i), Float.parseFloat(points[i]));
										tsRecord.addTuple(Point);
										break;
									case DOUBLE:
										Point = new DoubleDataPoint(sensorList.get(i), Double.parseDouble(points[i]));
										tsRecord.addTuple(Point);
										break;
									case BOOLEAN:
										Point = new BooleanDataPoint(sensorList.get(i), Boolean.parseBoolean(points[i]));
										tsRecord.addTuple(Point);
										break;
									case TEXT:
										Point = new StringDataPoint(sensorList.get(i), Binary.valueOf(points[i]));
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
                				LOGGER.error("write record error: {}, error csv: {}", e.getMessage(), csvFile.getAbsolutePath(), e);
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
  	}

}
