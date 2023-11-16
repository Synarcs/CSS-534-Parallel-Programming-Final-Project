package io.parallel.lbz.sequential;

import java.io.Serializable;
import java.util.*;

class SkilineObjects {
    private int x; private int y;
    SkilineObjects(int x, int y){
        this.x = x; this.y = y;
    }

    public int getX() { return x;}
    public int getY() { return y;}
}

// Spatial Skyline Query Se
public class Skiline implements Serializable {
    double[][] grid;
    int[][][] favourableFacilities;
    int[][][] unfavourableFacilities;

    double[][] distanceTable;

    private final boolean debug = true;

    // facility 1 favourness
    Skiline(int n, int m, int favFacilities, int unFavFacilites){
        this.grid = new double[n][m];
        this.favourableFacilities = new int[favFacilities][5][2];
        this.unfavourableFacilities = new int[unFavFacilites][1][2];
        /**
         *  z dimen stores the facility type
         *      each 2d array stores the coordinates for the required z facility.
         *
         *   I assume each facility has 5 coordinates that are favorable;
         *   I assume each facility has 1 coordinate that is unfavourable;
         */

        int totalFacilites =  favFacilities + unFavFacilites;
    }

    public void generateDistanceTable(int totalPoints, int totalFacilites){
        this.distanceTable = new double[totalPoints][totalFacilites];
    }

    public double calcDistance(int x, int y, int x1, int y1){
        return Math.sqrt((x1 - x) * (x1 - x) + (y1 - y) * (y1 - y));
    }

    public List<Integer> skylineObjectsPoints(int favCount, int unFavCount, int n, int m) {
        Set<Integer> objects = new HashSet<>();

        for (int i = 0; i < this.distanceTable.length; i++) {
            int currentHorzIndex = i;
            boolean isSkyline = true;  // check for the syline status for a given point
            for (int j = 0; j < this.distanceTable.length; j++) {
                if (i != j) {
                    int countDominance = 0;
                    for (int k = 0; k < favCount + unFavCount; k++) {
                        if (this.distanceTable[i][k] < this.distanceTable[j][k]) {
                            countDominance++;
                        }
                    }
                    if (countDominance == favCount + unFavCount) {
                        // Point i dominates point j in all dimensions
                        // only considering if there is domination for even a single point in the grid
                        isSkyline = false;
                        break;
                    }
                }
            }
            if (isSkyline) {
                objects.add(currentHorzIndex);
            }
        }
        if (objects.size() == this.distanceTable.length)
            System.out.println("All points are selected tge Graph is Dense and " +
                    "the each of the point is dominant over other considering all the features");
        System.out.println(objects);
        return new ArrayList<>(objects);
    }

    public void printDistanceTable(double[][] distanceTable){
        for (int i=0 ; i< distanceTable.length; i++)
            System.out.println(Arrays.toString(distanceTable[i]));
    }

    public void skilineAlgorithm(int favCount, int unFavCount, int gridX, int gridY, boolean isCustom){
        int n = this.grid.length; int m = this.grid[0].length;
        int pointsIntervalSelected = isCustom ? 2 : gridX * gridY;

        /*
                 count for the first facility
        */
        //       consider all the points in the grid too generate adensely populated network
        if (pointsIntervalSelected == gridX * gridY){
            this.generateDistanceTable(pointsIntervalSelected, favCount + unFavCount);
            /*
                entire points in the grid are considered the total grid size is n * m (the entire are of the graph Node)
                This would be much simple in parallel implementation
             */
            int distI = 0;
            // O(n ^ 4)
            for (int pointX = 0; pointX < n; pointX++){
                for (int pointY = 0; pointY < m;  pointY++) {
                    int currentPointX = pointX;
                    int currentPointY = pointY;
                    if (currentPointY >= n || currentPointY >= m) return;

                    for (int kFavour = 0; kFavour < this.favourableFacilities.length; kFavour++) {
                        double distance = Double.MAX_VALUE;
                        for (int points = 0; points < this.favourableFacilities[kFavour].length; points++) {
                            int favorX = this.favourableFacilities[kFavour][points][0];
                            int favorY = this.favourableFacilities[kFavour][points][1];
                            distance = Double.min(distance, calcDistance(currentPointX, currentPointY, favorX, favorY));
                        }
                        this.distanceTable[distI][kFavour] = distance;
                    }

                    for (int kunFavour = 0; kunFavour < this.unfavourableFacilities.length; kunFavour++) {
                        double distance = Double.MAX_VALUE;
                        for (int points = 0; points < this.favourableFacilities[kunFavour].length; points++) {
                            int favorX = this.favourableFacilities[kunFavour][points][0];
                            int favorY = this.favourableFacilities[kunFavour][points][1];
                            distance = Double.min(distance, calcDistance(currentPointX, currentPointY, favorX, favorY));
                        }
                        this.distanceTable[distI][favCount + kunFavour] = (-1) * distance;
                    }
                    distI++;
                }
                /*
                 * consider all the features type and take the minimum out of it for each of them
                 */
            }
            System.out.println("the total length for the 2d grid is" + this.distanceTable.length + " " + this.distanceTable[0].length);

            if (debug)
                printDistanceTable(this.distanceTable);
            skylineObjectsPoints(favCount, unFavCount, gridX, gridY);
        }else {
            System.out.println("some custom points are selected");
            int[][] pointCoordinates = new int[][]{{1, 5}, {1, 6}};
            this.generateDistanceTable(pointCoordinates.length, favCount + unFavCount);
            int distI = 0;
            // O(n ^ 3)
            for (int pointX = 0; pointX < pointCoordinates.length; pointX++){
                int currentPointX = pointCoordinates[pointX][0];
                int currentPointY = pointCoordinates[pointX][1];
                if (currentPointY >= n || currentPointY >= m) return;

                for (int kFavour = 0; kFavour < this.favourableFacilities.length; kFavour++) {
                        double distance = Double.MAX_VALUE;
                        for (int points = 0; points < this.favourableFacilities[kFavour].length; points++) {
                            int favorX = this.favourableFacilities[kFavour][points][0];
                            int favorY = this.favourableFacilities[kFavour][points][1];
                            distance = Double.min(distance, calcDistance(currentPointX, currentPointY, favorX, favorY));
                        }
                        this.distanceTable[distI][kFavour] = distance;
                    }

                    for (int kunFavour = 0; kunFavour < this.unfavourableFacilities.length; kunFavour++) {
                        double distance = Double.MAX_VALUE;
                        for (int points = 0; points < this.favourableFacilities[kunFavour].length; points++) {
                            int favorX = this.favourableFacilities[kunFavour][points][0];
                            int favorY = this.favourableFacilities[kunFavour][points][1];
                            distance = Double.min(distance, calcDistance(currentPointX, currentPointY, favorX, favorY));
                        }
                        this.distanceTable[distI][favCount + kunFavour] = (-1) * distance;
                    }
                    distI++;
                }
                if (debug)
                    printDistanceTable(this.distanceTable);
                skylineObjectsPoints(favCount, unFavCount, gridX, gridY);
            }
    }

    public static void main(String[] args) {
        int totalFavFacilitesTypes = 2;
        int unFavFaciliteType = 1;
        int gridX = 10;
        int gridY = 10;

        Skiline line = new Skiline(gridX, gridY , totalFavFacilitesTypes, unFavFaciliteType);
//        line.favourableFacilities[0] = new int[][]{{14, 49},{53,4},{43, 18}, {86, 17}, {1, 63}};
//        line.favourableFacilities[1] = new int[][]{{25, 1},{40,31}, {53, 80}, {29, 37}, {17, 97}};
        line.favourableFacilities[0] = new int[][]{{4, 2},{1, 0}, {3, 2}, {5, 8}, {2, 0}};
        line.favourableFacilities[1] = new int[][]{{2, 4},{6,7}, {5,1}, {3, 9}, {1 ,7}};

//        line.unfavourableFacilities[0] = new int[][]{{32, 88}, {70, 54}};
        line.unfavourableFacilities[0] = new int[][]{{1, 5}, {3, 7}};

        line.skilineAlgorithm(totalFavFacilitesTypes, unFavFaciliteType, gridX, gridY, true);
    }

}
