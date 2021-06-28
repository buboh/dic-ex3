package at.ac.tuwien.ec.scheduling.offloading.algorithms.dic3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.jgrapht.graph.DirectedAcyclicGraph;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.offloading.algorithms.heftbased.utils.NodeRankComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import scala.Tuple2;

/**
 * OffloadScheduler class that implements the Earliest-Time-First (ETF)
 * algorithm, a priority based scheduling algorithm for homogeneous systems.
 * 
 * Hwang J J, Chow Y C, Anger F D, Lee C Y, "Scheduling precedence graph with
 * interprocessor communication time", SIAM Journal of Computing, Vol.18, 1989.
 */

public class ETF extends OffloadScheduler {

    /**
     *
     * @param A MobileApplication property from SimIteration
     * @param I MobileCloudInfrastructure property from SimIteration Constructors
     *          set the parameters and calls setRank() to nodes' ranks
     */

    public ETF(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
        // compute B-level for all tasks
        setRank(this.currentApp, this.currentInfrastructure);
    }

    public ETF(Tuple2<MobileApplication, MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
        // compute B-level for all tasks
        setRank(this.currentApp, this.currentInfrastructure);
    }

    protected void setRank(MobileApplication A, MobileCloudInfrastructure I) {
        for (MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
            msc.setVisited(false);

        for (MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
            calculateBLevel(msc, A.getTaskDependencies(), I);
    }

    @Override
    public ArrayList<? extends OffloadScheduling> findScheduling() {
        // execution time
        double start = System.nanoTime();

        // ETF output: starting time, finishing time and processor for each task
        ArrayList<OffloadScheduling> schedulings = new ArrayList<OffloadScheduling>();

        // nodes with 0 incoming edges - ready to execute
        ArrayList<MobileSoftwareComponent> availableTasks = this.getMobileApplication().readyTasks();
        // PriorityQueue<MobileSoftwareComponent> availableTasks = new
        // PriorityQueue<MobileSoftwareComponent>(new NodeRankComparator());

        // We initialize a new OffloadScheduling object, modelling the scheduling
        // computer with this algorithm
        OffloadScheduling scheduling = new OffloadScheduling();

        // while scheduledTasks
        while (!availableTasks.isEmpty()) {

            double nextEst = Double.MAX_VALUE;
            MobileSoftwareComponent nextTask = null;
            ComputationalNode target = null;

            // compute EST for all tasks for all processors
            for (MobileSoftwareComponent currTask : availableTasks) {
                for (ComputationalNode cn : currentInfrastructure.getAllNodes()) {
                    if (isValid(scheduling, currTask, cn)) {
                        // Earliest Start Time EST
                        double est = cn.getESTforTask(currTask);
                        if (est < nextEst) {
                            nextEst = est;
                            nextTask = currTask;
                            target = cn;
                        } else if (est == nextEst) {
                            if (currTask.getRank() > nextTask.getRank()) {
                                nextEst = est;
                                nextTask = currTask;
                                target = cn;
                            }
                        }
                    }
                }
                // check if execution on local device is better
                ComputationalNode local = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
                if (isValid(scheduling, currTask, local)) {
                    // Earliest Start Time EST
                    double est = local.getESTforTask(currTask);
                    if (est < nextEst) {
                        nextEst = est;
                        nextTask = currTask;
                        target = local;
                    } else if (est == nextEst) {
                        if (currTask.getRank() > nextTask.getRank()) {
                            nextEst = est;
                            nextTask = currTask;
                            target = local;
                        }
                    }
                }
            }

            if (target != null && nextTask != null) {
                deploy(scheduling, nextTask, target);
                availableTasks.remove(nextTask);
                availableTasks.addAll(this.getMobileApplication().getNeighbors(nextTask));
            }

            /*
             * if simulation considers mobility, perform post-scheduling operations (default
             * is to update coordinates of mobile devices)
             */
            // if (OffloadingSetup.mobility)
            // postTaskScheduling(scheduling);
        }

        // execution time
        double end = System.nanoTime();
        scheduling.setExecutionTime(end - start);

        schedulings.add(scheduling);
        return schedulings;

    }

    /**
     * calculateBLevel is the task prioritizing phase of HLFET rank is computed
     * recuversively by traversing the task graph downward rank is calculated by
     * adding up the maximum weights in the graph until the exist task is reached
     * 
     * @param msc
     * @param dag            Mobile Application's DAG
     * @param infrastructure
     * @return the rank of msc
     */
    private double calculateBLevel(MobileSoftwareComponent msc,
            DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
            MobileCloudInfrastructure infrastructure) {
        double w_cmp = 0.0; // average execution time of task on each processor / node of this component
        /*
         * since upward Rank is defined recursively, visited makes sure no extra
         * unnecessary computations are done when calling calculateBLevel on all nodes
         * during initialization
         */
        if (!msc.isVisited()) {
            msc.setVisited(true);
            int numberOfNodes = infrastructure.getAllNodes().size() + 1;
            for (ComputationalNode cn : infrastructure.getAllNodes())
                w_cmp += msc.getLocalRuntimeOnNode(cn, infrastructure);

            w_cmp = w_cmp / numberOfNodes;

            double maxNeighbor = 0.0;
            // for the exit task rank=w_cmp
            for (ComponentLink neigh : dag.outgoingEdgesOf(msc)) {
                // rank = w_Cmp + max(cij + rank(j) for all j in succ(i)
                // where cij is the average commmunication cost of edge (i, j)
                double thisNeighbor = 0.0;
                thisNeighbor = calculateBLevel(neigh.getTarget(), dag, infrastructure);
                // succesor's computational weight (= Node Weight)

                double tmpCRank = 0;
                // this component's average Communication rank (= Edge Weight)
                // We consider only offloadable successors. If a successor is not offloadable,
                // communication cost is 0
                if (neigh.getTarget().isOffloadable()) {
                    for (ComputationalNode cn : infrastructure.getAllNodes())
                        tmpCRank += infrastructure.getTransmissionTime(neigh.getTarget(),
                                infrastructure.getNodeById(msc.getUserId()), cn);
                    tmpCRank = tmpCRank / (infrastructure.getAllNodes().size());
                }

                // Increase the cost of this Neighbor by the Communication (=Edge) Weight / Cost
                thisNeighbor = thisNeighbor + tmpCRank;

                if (thisNeighbor > maxNeighbor) {
                    maxNeighbor = thisNeighbor;
                }
            }

            double rank = w_cmp + maxNeighbor;
            msc.setRank(rank);
        }
        return msc.getRank();
    }

    /*
     * private class ESTComparator implements Comparator<MobileSoftwareComponent> {
     * 
     * private MobileCloudInfrastructure currentInfrastructure; private
     * OffloadScheduling currentScheduling;
     * 
     * private ESTComparator(MobileCloudInfrastructure I, OffloadScheduling
     * scheduling) { super(); this.currentInfrastructure = I; this.currentScheduling
     * = scheduling; }
     * 
     * @Override public int compare(MobileSoftwareComponent o1,
     * MobileSoftwareComponent o2) { double est1 = Double.MAX_VALUE; double est2 =
     * Double.MAX_VALUE; ComputationalNode cn1; ComputationalNode cn2; for
     * (ComputationalNode cn : currentInfrastructure.getAllNodes()) if
     * (isValid(currentScheduling, o1, cn)) { double est = cn.getESTforTask(o1); //
     * Earliest Start Time EST if (est < tMin) { tMin = est; target = cn; } }
     * 
     * return 0; } }
     */
}
