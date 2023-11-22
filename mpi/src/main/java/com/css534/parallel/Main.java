package com.css534.parallel;

import mpi.*;

class MRGASKYMPI {
    public static void main(String[] args) throws MPIException {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (rank == 0 && args.length != 3) {
            System.err.println("Usage: java MRGASKYMPI <numberOfFacilities> size size");
            MPI.Finalize();
            System.exit(1);
        }

        int[] params = new int[3];
        if (rank == 0) {
            params[0] = Integer.parseInt(args[0]);
            params[1] = Integer.parseInt(args[1]);
            params[2] = Integer.parseInt(args[2]);
        }

        MPI.COMM_WORLD.Bcast(params, 0, 3, MPI.INT, 0);

        int numberOfFacilities = params[0];
        int m = params[1];
        int n = params[2];

        System.out.println("rank:" + rank + " , numberOfFacilities:" + numberOfFacilities);
        System.out.println("rank:" + rank + " , m:" + m);
        System.out.println("rank:" + rank + " , n:" + n);

        double[][][] localFacilityGrid = new double[numberOfFacilities][m][n];

        if (MPI.COMM_WORLD.Rank() == 0) {
            localFacilityGrid = initializeArray(numberOfFacilities, m, n);
        }

        MPI.COMM_WORLD.Bcast(localFacilityGrid, 0, numberOfFacilities * m * n, MPI.DOUBLE, 0);

        if (MPI.COMM_WORLD.Rank() == 0) {
            System.out.println("Before MRGASKY:");
            printArray(localFacilityGrid, numberOfFacilities, m, n);
        }

        if (MPI.COMM_WORLD.Rank() == 0) {
            //  MRGASKY algorithm
            for (int facility = 0; facility < numberOfFacilities; facility++) {
                for (int row = 0; row < m; row++) {
                    // Calculate Euclidean distance from left to right
                    double[] dist_left_right = calculateDistanceLeftRight(localFacilityGrid[facility][row]);

                    // Calculate Euclidean distance from right to left
                    double[] dist_right_left = calculateDistanceRightLeft(localFacilityGrid[facility][row]);

                    // Update the grid based on the minimum distance
                    for (int col = 0; col < n; col++) {
                        localFacilityGrid[facility][row][col] = Math.min(dist_left_right[col], dist_right_left[col]);
                    }
                }
            }
        }

        if (MPI.COMM_WORLD.Rank() == 0) {
            System.out.println("After MRGASKY:");
            printArray(localFacilityGrid, numberOfFacilities, m, n);
        }

        MPI.Finalize();
    }

    private static double[][][] initializeArray(int numberOfFacilities, int m, int n) throws MPIException {
        double[][][] facilityGrid = new double[numberOfFacilities][m][n];
        java.util.Random rand = new java.util.Random(0);

        for (int facility = 0; facility < numberOfFacilities; facility++) {
            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {

                    facilityGrid[facility][i][j] = 0;

                    if (rand.nextDouble() < 0.1) {
                        facilityGrid[facility][i][j] = 1;
                    }
                }
            }
        }
        return facilityGrid;
    }

    // Function to calculate Euclidean distance from left to right for a single row
    private static double[] calculateDistanceLeftRight(double[] row) {
        double[] dist_left_right = new double[row.length];
        double dist = Double.MAX_VALUE;
        for (int col = 0; col < row.length; col++) {
            if (row[col] == 1.0) {
                dist = 0.0;
            } else if (dist >= 0.0) {
                dist++;
            } else if (row[col] == 0.0) {
                dist = Double.MAX_VALUE;
            }

            dist_left_right[col] = dist;
        }

        return dist_left_right;
    }

    // Function to calculate Euclidean distance from right to left for a single row
    private static double[] calculateDistanceRightLeft(double[] row) {
        double[] dist_right_left = new double[row.length];
        double dist = Double.MAX_VALUE;
        for (int col = row.length - 1; col >= 0; col--) {
            if (row[col] == 1.0) {
                dist = 0.0;
            } else if (dist >= 0.0) {
                dist++;
            } else if (row[col] == 0.0) {
                dist = Double.MAX_VALUE;
            }
            dist_right_left[col] = dist;
        }
        return dist_right_left;
    }

    private static void printArray(double[][][] facilityGrid, int numberOfFacilities, int m, int n) {
        for (int facility = 0; facility < numberOfFacilities; facility++) {
            System.out.println("Facility " + facility + ":");
            for (int i = 0; i < m; i++) {
                for (int j = 0; j < n; j++) {
                    System.out.print(facilityGrid[facility][i][j] + " ");
                }
                System.out.println();
            }
            System.out.println();
        }
    }
}
