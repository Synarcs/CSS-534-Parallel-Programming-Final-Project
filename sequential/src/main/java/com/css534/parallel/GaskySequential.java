package com.css534.parallel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

// Sequential implementation using Brute force.
public class GaskySequential implements Serializable {
    public static class Minmax {
        double min;
        double max;
        int i;
        int j;
    }

    private static double[][][] initializeArray(int numberOfFacilities, int m, int n, String filePath)
            throws IOException {
        double[][][] facilityGrid = new double[numberOfFacilities][m][n];
        int facilityIndex;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);

                // Read facility name, row, and grid values
                String facilityName = tokenizer.nextToken();
                int row = Integer.parseInt(tokenizer.nextToken()) - 1;

                // the grid values are in the format "00000000"
                String gridValues = tokenizer.nextToken();

                // Convert gridValues to a double array
                double[] rowValues = new double[gridValues.length()];
                for (int i = 0; i < gridValues.length(); i++) {
                    rowValues[i] = Character.getNumericValue(gridValues.charAt(i));
                }

                // Populate the facilityGrid array
                if (facilityName.contains("-")) {
                    facilityIndex = Integer.parseInt(facilityName.substring(1, 2)) - 1;
                } else {
                    facilityIndex = Integer.parseInt(facilityName.substring(1)) - 1;
                }

                for (int j = 0; j < n; j++) {
                    facilityGrid[facilityIndex][row][j] = rowValues[j];
                }
            }
        }

        return facilityGrid;
    }

    private static double[][][] positionFacilities(double[][][] FacilityGrid) {
        for (int k=0; k < FacilityGrid.length; k++){
            for (int i=0 ; i < FacilityGrid[0].length; i++){
                for (int j=0; j < FacilityGrid[0][0].length; j++){
                    if (FacilityGrid[k][i][j] == 1)
                        FacilityGrid[k][i][j] = 0;
                    else
                        FacilityGrid[k][i][j] = Double.MAX_VALUE;
                }
            }
        }
        return FacilityGrid;
    }

    private static double findEuclideanDistance(int x, int y, int x1, int y1){return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));}

    public static void updateRelativePositions(int row, int col, int k, double[][][] FacilityGrid,
                                                int gridSize){
        for (int i=0; i < gridSize; i++){
            for (int j=0 ; j < gridSize; j++){
                    double dist = Double.min(
                            FacilityGrid[k][i][j], findEuclideanDistance(row, col, i, j)
                    );
                    FacilityGrid[k][i][j] = dist;
            }
        }
    }

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        if (args.length != 5) {
            System.err.println("Usage: java MRGASKY <numberOfFacilities> size size favourablecount unfavourablecount");
            System.exit(1);
        }
        long time = System.currentTimeMillis();

        int numberOfFacilities = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[2]);
        int m = n;
        int fav = Integer.parseInt(args[3]);
        int unfav = Integer.parseInt(args[4]);

        double[][][] FacilityGrid = new double[fav + unfav][m][n];
        double[][][] FavourableFacilityGrid = new double[fav][m][n];
        double[][][] UnFavourableFacilityGrid = new double[unfav][m][n];


        try {
            String filePath = "input.txt";

            // Initialize the FacilityGrid array
            FacilityGrid = initializeArray(numberOfFacilities, m, n, filePath);
            FacilityGrid = positionFacilities(FacilityGrid);


            for (int k=0 ; k < FacilityGrid.length; k++){
                for (int i = 0; i < FacilityGrid[0].length; i++){
                    for (int j=0; j < FacilityGrid[0][0].length; j++){
                        if (FacilityGrid[k][i][j] == 0){
                            updateRelativePositions(i, j, k, FacilityGrid, n);
                        }
                    }
                }
            }

            for (int i = 0; i < fav; i++) {
                for (int j=0; j < n; j++){
                    for (int k=0; k < n; k++){
                        FavourableFacilityGrid[i][j][k] = FacilityGrid[i][j][k];
                    }
                }
            }

            for (int uf=fav; uf < FacilityGrid.length; uf++){
                for (int j=0; j < n; j++){
                    for (int k=0; k < n; k++){
                        UnFavourableFacilityGrid[uf - fav][j][k] = FacilityGrid[uf][j][k];
                    }
                }
            }

            // Step-3 Algorithm - Global Skyline Objects
            List<Minmax> minmaxFavList = new ArrayList<>();

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    Double local_min_val = Double.MAX_VALUE;
                    Double local_max_val = Double.MIN_VALUE;
                    Minmax minmax = new Minmax();
                    for (int k = 0; k < fav; k++) {
                        local_min_val = Double.min(local_min_val, FavourableFacilityGrid[k][i][j]);
                        local_max_val = Double.min(local_max_val, FavourableFacilityGrid[k][i][j]);
                    }
                    minmax.i = i;
                    minmax.j = j;
                    minmax.min = local_min_val;
                    minmax.max = local_max_val;
                    minmaxFavList.add(minmax);
                }
            }

            List<Minmax> minmaxUnFavList = new ArrayList<>();

            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    Double local_min_val = Double.MAX_VALUE;
                    Double local_max_val = Double.MIN_VALUE;
                    Minmax minmax = new Minmax();
                    for (int k = 0; k < unfav; k++) {
                        local_min_val = Double.min(local_min_val, UnFavourableFacilityGrid[k][i][j]);
                        local_max_val = Double.max(local_max_val, UnFavourableFacilityGrid[k][i][j]);
                    }
                    minmax.i = i;
                    minmax.j = j;
                    minmax.min = local_min_val;
                    minmax.max = local_max_val;
                    minmaxUnFavList.add(minmax);
                }
            }

            boolean isEqual = minmaxUnFavList.size() == minmaxFavList.size();
            if (isEqual) {
                boolean isGrid = minmaxUnFavList.size() == m * n;
                if (!isGrid) return;
            }else {
                System.out.println("error incorrect order of teh grid projected");
                return;
            }
            boolean debug= false;
            if (debug){
                System.out.println("min and max index 1  , 1 " + minmaxFavList.get(0).min + " " + minmaxFavList.get(0).max);
                System.out.println("min and max index 1  , 1 " + minmaxUnFavList.get(0).min + " " + minmaxUnFavList.get(0).max);
                System.out.println("the status for them is " + isEqual);
            }
            // O (N)
            for (int i=0 ; i < n * n ; i++){
                // use the struct for faster memory processing as compared o memory jumps required for a 2d array case
                double globalMaxIndexFav = minmaxFavList.get(i).max;
                double globalMinimaIndexFav = minmaxFavList.get(i).min;
                int favi = minmaxFavList.get(i).i + 1; int favj = minmaxFavList.get(i).j + 1;

                double globalMinimaIndexUnFav = minmaxUnFavList.get(i).min;
                double globalMaxIndexUnFav = minmaxUnFavList.get(i).max;


                if (globalMinimaIndexFav != Double.MAX_VALUE && globalMinimaIndexUnFav != Double.MAX_VALUE) {
                    if (globalMinimaIndexUnFav < globalMinimaIndexFav ||
                            globalMinimaIndexFav == globalMinimaIndexUnFav ||
                            globalMaxIndexFav > globalMinimaIndexUnFav ||
                            globalMinimaIndexFav > globalMaxIndexUnFav) {
                        continue;
                    } else {
                        System.out.println(favi + " " + favj);
                    }
                }
            }
            System.out.println("Elapsed Time: " + (System.currentTimeMillis() - startTime));
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }
}
