package com.css534.parallel;

import java.io.Serializable;
import java.util.*;

class Sequential implements Serializable {
    private Map<Integer, HashSet<Integer>> adjList;
    private Map<Integer, HashSet<Integer>> communities;
    private int totalEdges = 0;
    private int[] nodeDegrees;

    /*
        contains a un directed graph and also unweighted
        Note while parallelization each graph will receive a subset of the edges to parallely load in memory
     */

    public void generateGraph(int nodes, int[][] edges){
        this.adjList = new HashMap<>();
        // lets assume a 1 index based graph
        for (int i=1; i <= nodes; i++)
            this.adjList.put(i, new HashSet<>());

        for (int[] edge: edges){
            int src = edge[0]; int dest = edge[1];
            this.adjList.get(src).add(dest);
        }
        setTotalEdges(edges);
    }

    // sum of all the in dregrees for the graph
    public void setTotalEdges(int[][] edges){ this.totalEdges = edges.length; }
    public void generateCommunities(int nodes){
        this.communities = new HashMap<>();
        for (int i=1; i <= nodes; i++)
            this.communities.put(i, new HashSet<>());
        for (int i=1; i <= nodes; i++)
            this.communities.get(i).add(i);
    }

    public void generateNodeDegrees(int nodesStart, int nodesEnd){
        this.nodeDegrees = new int[nodesEnd + 1]; // use the parition index values
        for (int i = 1; i <= nodesEnd; i++)
            this.nodeDegrees[i] = this.adjList.get(i).size();
    }
    public int getTotalEdges() {return totalEdges;}

    public Map<Integer, HashSet<Integer>> getCommunities() {return communities;}

    public int[] getNodeDegrees() {return nodeDegrees;}

    /*
        Assume each node belongs to the different community for init computation
     */
    public double getInitModularities(int nodes){
        // calculate based on the communiteis present
        double[] modularities = new double[nodes + 1];
        System.out.println(Arrays.toString(this.nodeDegrees));
        // not ocnsidering other edges since the community function is always 0 because of different communities being present and chronicle function equals 1
        for (Map.Entry<Integer, HashSet<Integer>> val : this.communities.entrySet()){
            int node = val.getKey();
            double edge = (double) 1 / (double) (2 * getTotalEdges());
            double Aij = 0.0;
            double range = (Aij - ((double) (nodeDegrees[node] * nodeDegrees[node]) / (double) (2 * getTotalEdges())));
            double communityAfection = (double) 1;
            modularities[node] = edge * range * communityAfection;
        }

        double totalInitModularity = 0.0;
        double maxModularity = Double.MIN_VALUE;
        for (double xx: modularities){
            totalInitModularity += xx;
            maxModularity = Double.max(maxModularity, xx);
        }
        System.out.println("the disjoint modularity set for the graph is " + totalInitModularity + " " + Arrays.toString(modularities));

        return  totalInitModularity;
    }

    public void cleanDanglingCommunities(){
        for (Integer community: this.communities.keySet()){
            if (this.communities.get(community).size() == 0)
                this.communities.remove(community); // remove any orphan communities
        }
    }

    //    Q = 1/(2m) * Σᵢⱼ ([ Aᵢⱼ — kᵢkⱼ / (2m)] * δ(cᵢ, cⱼ))
    public double reComputeModularity(double olderModularity, int currentTempMoveNode, HashSet<Integer> newCommunity) {
        double deltaQ = 0.0; // compute the total sum of the modularity functions
        System.out.println("The current function status is  " + currentTempMoveNode + " " + newCommunity);
        // Calculate the change in modularity for the current node
        for (int neighbor : adjList.get(currentTempMoveNode)) {
            //
            int ki = nodeDegrees[currentTempMoveNode];
            int kj = nodeDegrees[neighbor];
            double Aij = adjList.get(currentTempMoveNode).contains(neighbor) ? 1.0 : 0.0;

            double term1 = Aij - ((ki * kj) / (2.0 * getTotalEdges()));
            double term2 = (newCommunity.contains(currentTempMoveNode) ? 1.0 : 0.0) - (ki / (2.0 * getTotalEdges()));
            double term3 = (newCommunity.contains(neighbor) ? 1.0 : 0.0) - (kj / (2.0 * getTotalEdges()));

            deltaQ += term1 - term2 - term3;
        }

        // Calculate the new modularity
        double newModularity = olderModularity + deltaQ;
        return newModularity;
    }

    public void performOptimizations(int nodes, double olderGraphModularity){
        System.out.println(this.adjList);
        for (int node = 1; node <= nodes; node++){
            // {1: {1}, 2: {2}, 3: {3, 1}}
//            {1: {2, 4 , 6}
            int currentNode = node;

            // loop through all its neighbours to perform community optimizations
            for (Integer neighbours: this.adjList.get(currentNode)){
                this.communities.get(neighbours).add(currentNode);
                double newModularity = reComputeModularity(olderGraphModularity, currentNode, this.communities.get(neighbours));
                if (newModularity <= olderGraphModularity){
                    this.communities.get(neighbours).remove(currentNode); // back track and remove if it did find a good friends with neighbours;
                }
                System.out.println("new modularity is" + newModularity + " " + "current disjoint  communities " + this.communities);
            }
        }
        cleanDanglingCommunities();
        System.out.println(this.communities);
    }

    public static void main(String[] args) {
        final int nodes = 6;
        Sequential graph = new Sequential();
        int[][] edges = {{1, 2}, {1, 3}, {2, 3}, {3, 4}, {4, 5}, {4, 6}, {5, 6}};
        graph.generateGraph(nodes, edges);
        System.out.println(graph.adjList);
        graph.generateCommunities(nodes);
        graph.generateNodeDegrees(nodes,6);
        System.out.println(graph.communities);
        double initOlderModularity = graph.getInitModularities(nodes);
        System.out.println(initOlderModularity);
        graph.performOptimizations(nodes, initOlderModularity);
    }
}
