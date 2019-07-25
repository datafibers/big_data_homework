package com.datafibers.aws.homework;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import ucar.ma2.*;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

/**
 * NetCDF Reader which currently only read time, forecast time, y/latitude, x/longitude, and customized key variable
 */
public class NetCDFReader {

    static final String VAR_REALIZATION = "realization";
    static final String VAR_TIME = "time";
    static final String VAR_FORECAST_TIME = "forecast_reference_time";

    /**
     * Get key measure from the NetCDF and output data in a output json file for every 1000k lines (size around 10M)
     * @param fileName netcdf file name and path
     * @param measure key variable to read, such as air_temperature
     * @param coord a comma separated string to specify coordination system,
     *              such as latitude,longitude or projection_y_coordinate,projection_x_coordinate
     * @param limit the number of realization to read. -1 reads all.
     * @param downloadDir where to keep the output json
     */
    public static void getMeasureAsFile(String fileName, String measure, String coord, int limit, String downloadDir) {
        BufferedWriter bw;
        try {
            int rLength = 0;
            int yLength = 0;
            int xLength = 0;
            NetcdfFile dataFile = NetcdfFile.open(fileName, null);
            System.out.println(dataFile.toString());
            System.out.println(dataFile.getDimensions());
            for(Dimension dim : dataFile.getDimensions()) {
                rLength = ((limit < 0) && dim.getFullName().equalsIgnoreCase(VAR_REALIZATION)) ? dim.getLength() : limit;
                if(dim.getFullName().equalsIgnoreCase(coord.split(",")[0])) yLength = dim.getLength();
                if(dim.getFullName().equalsIgnoreCase(coord.split(",")[1])) xLength = dim.getLength();
            }

            // read the lat/lon and temperature variables
            Variable measureVar = dataFile.findVariable(measure);
            Variable realizationVar = dataFile.findVariable(VAR_REALIZATION);
            Variable yVar = dataFile.findVariable(coord.split(",")[0]);
            Variable xVar = dataFile.findVariable(coord.split(",")[1]);

            // Read the valid time and forecast time
            Variable time = dataFile.findVariable(VAR_TIME);
            ArrayLong.D0 timeCoord = (ArrayLong.D0) time.read();

            Variable forecastTime = dataFile.findVariable(VAR_FORECAST_TIME);
            ArrayLong.D0 forecastTimeCoord = (ArrayLong.D0) forecastTime.read();

            // Read the latitude and longitude coordinate variables into arrays
            ArrayFloat.D1 yCoordArray = (ArrayFloat.D1) yVar.read();
            ArrayFloat.D1 xCoordArray = (ArrayFloat.D1) xVar.read();

            int measureVarDimSize = measureVar.getDimensions().size();

            // iterate through the arrays, do something with the data
            int lineNo = 1;
            String outFileNameBase = fileName + "_line_start";
            String outFileName = outFileNameBase + "_000000";
            bw = new BufferedWriter(new FileWriter(new File(outFileName)));
            int cnt = 0;
            for(int r = 0; r < rLength; r++) {

                // iterate through 1 realization a time to save memory
                ArrayFloat.D2 measureArray = (measureVarDimSize == 3) ?
                        (ArrayFloat.D2) measureVar.read(new int[]{r, 0, 0}, new int[]{1, yLength, xLength}).reduce(0) :
                        (ArrayFloat.D2) measureVar.read(new int[]{r, 0, 0, 0}, new int[]{1, 1, yLength, xLength}).reduce(0).reduce(0);

                for (int y = 0; y < yLength; y++) {
                    for (int x = 0; x < xLength; x++) {
                        if(lineNo++%100000 == 0) {
                            bw.flush();
                            bw.close();

                            FileUtils.moveFile(FileUtils.getFile(outFileName),
                                    FileUtils.getFile(downloadDir + "/" + outFileName + ".json"));
                            outFileName = outFileNameBase + "_" + lineNo;
                            //System.out.println("new outfilename = " + outFileName);
                            bw = new BufferedWriter(new FileWriter(new File(outFileName)));
                        }

                        // do something with the data
                        System.out.print(yCoordArray.get(y) + ",");
                        System.out.print(xCoordArray.get(x) + ",");
                        System.out.print(timeCoord.get() + ",");
                        System.out.print(forecastTimeCoord.get() + ",");
                        System.out.print("[" + r + "," + y + "," + x + "," +  (cnt++) + "],");
                        System.out.println(measureArray.get(y, x));
                        bw.write(new JSONObject()
                                .put("y_coord", yCoordArray.get(y) + "")
                                .put("x_coord", xCoordArray.get(x) + "")
                                .put("time", timeCoord.get() + "")
                                .put("forecast", forecastTimeCoord.get() + "")
                                .put(measure, measureArray.get(y, x) + "").toString() + "\n");

                    }
                }
                bw.flush();
                bw.close();
                FileUtils.moveFile(FileUtils.getFile(outFileName), FileUtils.getFile(outFileName + ".json"));
            }

            dataFile.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Get key measure/variable from the NetCDF and output to console
     * @param fileName netcdf file name and path
     * @param measure key variable to read, such as air_temperature
     * @param coord a comma separated string to specify coordination system,
     *              such as latitude,longitude or projection_y_coordinate,projection_x_coordinate
     * @param limit the number of realization to read. -1 reads all.
     */
    public static void getMeasureToConsole(String fileName, String measure, String coord, int limit) {

        try {
            int rLength = 0;
            int yLength = 0;
            int xLength = 0;
            NetcdfFile dataFile = NetcdfFile.open(fileName, null);
            System.out.println(dataFile.toString());
            System.out.println(dataFile.getDimensions());
            for(Dimension dim : dataFile.getDimensions()) {
                rLength = ((limit < 0) && dim.getFullName().equalsIgnoreCase(VAR_REALIZATION)) ? dim.getLength() : limit;
                if(dim.getFullName().equalsIgnoreCase(coord.split(",")[0])) yLength = dim.getLength();
                if(dim.getFullName().equalsIgnoreCase(coord.split(",")[1])) xLength = dim.getLength();
            }

            // read the lat/lon and temperature variables
            Variable measureVar = dataFile.findVariable(measure);
            Variable yVar = dataFile.findVariable(coord.split(",")[0]);
            Variable xVar = dataFile.findVariable(coord.split(",")[1]);

            // Read the valid time and forecast time
            Variable time = dataFile.findVariable(VAR_TIME);
            ArrayLong.D0 timeCoord = (ArrayLong.D0) time.read();

            Variable forecastTime = dataFile.findVariable(VAR_FORECAST_TIME);
            ArrayLong.D0 forecastTimeCoord = (ArrayLong.D0) forecastTime.read();

            // Read the latitude and longitude coordinate variables into arrays
            ArrayFloat.D1 yCoordArray = (ArrayFloat.D1) yVar.read();
            ArrayFloat.D1 xCoordArray = (ArrayFloat.D1) xVar.read();

            int measureVarDimSize = measureVar.getDimensions().size();

            // iterate through the arrays, do something with the data
            int cnt = 0;
            for(int r = 0; r < rLength; r++) {

                // iterate through 1 realization a time to save memory
                ArrayFloat.D2 measureArray = (measureVarDimSize == 3) ?
                        (ArrayFloat.D2) measureVar.read(new int[]{r, 0, 0}, new int[]{1, yLength, xLength}).reduce(0) :
                        (ArrayFloat.D2) measureVar.read(new int[]{r, 0, 0, 0}, new int[]{1, 1, yLength, xLength}).reduce(0).reduce(0);

                for (int y = 0; y < yLength; y++) {
                    for (int x = 0; x < xLength; x++) {
                        // do something with the data
                        System.out.print(yCoordArray.get(y) + ",");
                        System.out.print(xCoordArray.get(x) + ",");
                        System.out.print(timeCoord.get() + ",");
                        System.out.print(forecastTimeCoord.get() + ",");
                        System.out.print("[" + r + "," + y + "," + x + "," +  (cnt++) + "],");
                        System.out.println(measureArray.get(y, x));
                    }
                }
            }

            dataFile.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws Exception {

        String[] fileNames = new String[] {
                "SkyWise-Global7km-GFSAviation_20170120_1200.nc",
                "rainfall_rate_2019-07-19T16:18:22Z_b57e94070031194d85bf916cb4d9f55396a25cc8.nc",
                "00001b1de284ad76ff1498690261c8cd8cb2b197.nc", //lwe_snowfall_rate
                "0005b322f03549730983662242c719d3f61bec99.nc",  //relative_humidity - 1.17G
                "air_temperature_2019-07-19T16:59:55Z_231652dae4a56aa67641279d094adb263168742d.nc",
                "lwe_thickness_of_snowfall_amount.nc",
                "lwe_snowfall_rate.nc"
        };

        NetCDFReader.getMeasureToConsole(
                fileNames[5],
                "air_pressure_at_sea_level",
                "projection_y_coordinate,projection_x_coordinate", //"latitude,longitude",
                1
        );

    }
}
