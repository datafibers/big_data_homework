package com.datafibers.aws.homework;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayLong;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Function to be wrapped in the map function to process each netcdf
 */
public class SparkNETCDFMapPartition implements Function<String, List<String>> {
    static final String VAR_REALIZATION = "realization";
    static final String VAR_TIME = "time";
    static final String VAR_FORECAST_TIME = "forecast_reference_time";

    @Override
    public List<String> call(String fileParameters) {
        List<String> payload = new ArrayList<>();
        String payloadRow;
        String[] fileParameterArray = fileParameters.split(",");

        //The file parameters got as "file_name,measure,coordy,coordx,limit")
        String fileName = fileParameterArray[0];
        String fileNameNoPath = Paths.get(fileName).getFileName().toString();
        String measure = fileParameterArray[1];
        String coordy = fileParameterArray[2];
        String coordx = fileParameterArray[3];
        int limit = Integer.parseInt(fileParameterArray[4]);

        try {
            int rLength = 0;
            int yLength = 0;
            int xLength = 0;
            NetcdfFile dataFile = NetcdfFile.open(fileName, null);

            // Find length for all key dimensions
            for (Dimension dim : dataFile.getDimensions()) {
                rLength = ((limit < 0) && dim.getFullName().equalsIgnoreCase(VAR_REALIZATION)) ? dim.getLength() : limit;
                if (dim.getFullName().equalsIgnoreCase(coordy)) yLength = dim.getLength();
                if (dim.getFullName().equalsIgnoreCase(coordx)) xLength = dim.getLength();
            }

            // read the coordination and key variables
            Variable measureVar = dataFile.findVariable(measure);
            Variable yVar = dataFile.findVariable(coordy);
            Variable xVar = dataFile.findVariable(coordx);

            // Read the valid time and forecast time
            Variable time = dataFile.findVariable(VAR_TIME);
            ArrayLong.D0 timeCoord = (ArrayLong.D0) time.read();

            Variable forecastTime = dataFile.findVariable(VAR_FORECAST_TIME);
            ArrayLong.D0 forecastTimeCoord = (ArrayLong.D0) forecastTime.read();

            // Read coordinate variables into arrays
            ArrayFloat.D1 yCoordArray = (ArrayFloat.D1) yVar.read();
            ArrayFloat.D1 xCoordArray = (ArrayFloat.D1) xVar.read();

            // Get how many dimensions needed to calculate the measure
            int measureVarDimSize = measureVar.getDimensions().size();

            // iterate through the arrays to process data
            int cnt = 0;
            for (int r = 0; r < rLength; r++) {

                /*
                 Iterate through 1 realization a time to save memory.
                 Here, we support three or four dimension.
                 Usually, we reduce 4 to 3, 3 to 2 dimensions to keep only variables we interest
                  */
                ArrayFloat.D2 measureArray = (measureVarDimSize == 3) ?
                        (ArrayFloat.D2) measureVar.read(new int[]{r, 0, 0}, new int[]{1, yLength, xLength}).reduce(0) :
                        (ArrayFloat.D2) measureVar.read(new int[]{r, 0, 0, 0}, new int[]{1, 1, yLength, xLength}).reduce(0).reduce(0);

                for (int y = 0; y < yLength; y++) {
                    for (int x = 0; x < xLength; x++) {
                        // put data to payload with schema fileName, yCoord, xCoord, time, forecast, measureName, measureValue
                        payloadRow = StringUtils.join(
                                Arrays.asList(
                                        fileNameNoPath,
                                        yCoordArray.get(y),
                                        xCoordArray.get(x),
                                        timeCoord.get(),
                                        forecastTimeCoord.get(),
                                        measure,
                                        measureArray.get(y, x)
                                ), ","
                        );
                        System.out.println(payloadRow + ",[" + r + "," + y + "," + x + "," + (cnt++) + "]");
                        payload.add(payloadRow);
                    }
                }
            }

            dataFile.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return payload;
    }
}
