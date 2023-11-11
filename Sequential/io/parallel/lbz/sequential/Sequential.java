package io.parallel.lbz.sequential;
import org.apache.commons.math3.linear.Array2DRowFieldMatrix;

import java.io.Serializable;

public class Sequential implements Serializable {

    /*
          Macroscopic constants from Naive stoke's consideration
          Density:
            ρ = ∑ᵢ fᵢ
          Velocities:
            u = 1/ρ ∑ᵢ fᵢ cᵢ
          Equilibrium:
            fᵢᵉ = ρ Wᵢ (1 + 3 cᵢ ⋅ u + 9/2 (cᵢ ⋅ u)² − 3/2 ||u||₂²)
          BGK Collision: (Lattice Boundry collusion).
            fᵢ ← fᵢ − ω (fᵢ − fᵢᵉ)
     */

    public int[] density(int[][] discreteVelocityes){
        int[] densities = new int[discreteVelocityes.length];
        for (int i=0; i < discreteVelocityes.length; i++){
            int sum = 0;
            for (int j=0; j < discreteVelocityes[0].length; j++){
                sum += discreteVelocityes[i][j];
            }
            densities[i] = sum;
        }
        return densities;
    }

    public void runSimulation() {
        for (int t = 0; t < Consts.ITERATIONS; t++){

        }
    }

    public static void main(String[] args) {
        Sequential simulator = new Sequential();

    }

    @Override
    public String toString() {
        return "LBZ Seimuation class" + Sequential.class.getName();
    }

}