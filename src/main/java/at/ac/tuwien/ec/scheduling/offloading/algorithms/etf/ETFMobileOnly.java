package at.ac.tuwien.ec.scheduling.offloading.algorithms.etf;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.jgrapht.graph.DirectedAcyclicGraph;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import scala.Tuple2;

/**
 * OffloadScheduler class that implements the Earliest-Time-First (ETF)
 * algorithm, a priority based scheduling algorithm for homogeneous systems.
 * 
 * Hwang J J, Chow Y C, Anger F D, Lee C Y, "Scheduling precedence graph with
 * interprocessor communication time", SIAM Journal of Computing, Vol.18, 1989.
 */

public class ETFMobileOnly extends OffloadScheduler {

    private static final long serialVersionUID = 1L;

    /**
     *
     * @param A MobileApplication property from SimIteration
     * @param I MobileCloudInfrastructure property from SimIteration Constructors
     *          set the parameters and calls setRank() to nodes' ranks
     */

    public ETFMobileOnly(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
        // compute B-level for all tasks
        setRank(this.currentApp, this.currentInfrastructure);
        System.out.println("ETF created");
    }

    public ETFMobileOnly(Tuple2<MobileApplication, MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
        // compute B-level for all tasks
        setRank(this.currentApp, this.currentInfrastructure);
        System.out.println("ETF created");
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

        // queue to hold currently scheduled tasks
        PriorityQueue<MobileSoftwareComponent> scheduledTasks = new PriorityQueue<MobileSoftwareComponent>(
                new RuntimeComparator());

        // list to hold final scheduling
        ArrayList<OffloadScheduling> schedulings = new ArrayList<OffloadScheduling>();

        // Set of tasks with 0 incoming edges - ready to execute
        HashSet<MobileSoftwareComponent> availableTasks = new HashSet<>();
        availableTasks.addAll(this.getMobileApplication().readyTasks());

        // reset visited to use for scheduling (was previously used to compute b-level)
        ArrayList<MobileSoftwareComponent> allTasks = this.getMobileApplication().getTasks();
        allTasks.forEach(task -> task.setVisited(false));

        // Print how many tasks we want to schedule
        System.out.println("Total tasks: " + allTasks.size());
        System.out.println(allTasks);
        // debug counter holding the number of already scheduled tasks
        int schedulingCounter = 0;

        // We initialize a new OffloadScheduling object, modelling the scheduling
        // computer with this algorithm
        OffloadScheduling scheduling = new OffloadScheduling();

        // while there are tasks to be scheduled
        while (!availableTasks.isEmpty()) {
            // the values we are trying to find:
            // lowest EST and the corresponding task and node
            double nextEst = Double.MAX_VALUE;
            MobileSoftwareComponent nextTask = null;
            ComputationalNode target = null;

            // compute EST for all tasks for all processors
            // then deploy task with the lowest overall EST and highest blevel
            for (MobileSoftwareComponent currTask : availableTasks) {
                // check if execution on local device is possible
                ComputationalNode local = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
                // check if task is compatible with the computational node
                if (isValid(scheduling, currTask, local)) {
                    // get Earliest Start Time EST for the current pair
                    double est = local.getESTforTask(currTask);
                    // check if this pair has the lowest EST until now
                    if (est < nextEst) {
                        // if true, select task-node pair to be scheduled next
                        nextEst = est;
                        nextTask = currTask;
                        target = local;
                    } else if (est == nextEst) {
                        // break ties with b-level
                        if (currTask.getRank() > nextTask.getRank()) {
                            nextEst = est;
                            nextTask = currTask;
                            target = local;
                        }
                    }
                }
            }

            // if there is a target and a nextTask to be scheduled
            if (target != null && nextTask != null) {
                // deploy the task
                deploy(scheduling, nextTask, target);
                // and keep track of it
                scheduledTasks.add(nextTask);
                // then remove it from the available tasks and set it as visited
                availableTasks.remove(nextTask);
                nextTask.setVisited(true);
                // also count that a task has been scheduled
                schedulingCounter++;

                // now get all ready successors of the just scheduled task
                MobileApplication ma = this.getMobileApplication();
                ArrayList<MobileSoftwareComponent> neighbors = ma.getNeighbors(nextTask);

                // check if all predecessors have already been visited, so that the DAG order is preserved
                List<MobileSoftwareComponent> readyNeigbors = neighbors.stream()
                        .filter(n -> ma.getPredecessors(n).stream().allMatch(pred -> pred.isVisited()))
                        .collect(Collectors.toList());

                availableTasks.addAll(readyNeigbors);

            } 
            if (!scheduledTasks.isEmpty()) {
                MobileSoftwareComponent terminated = scheduledTasks.remove();
                ComputationalNode prevTarget = (ComputationalNode) scheduling.get(terminated);
                prevTarget.undeploy(terminated);
                // System.out.println("Undeploy: " + terminated + " from " + prevTarget);
            }

            /*
             * if simulation considers mobility, perform post-scheduling operations (default
             * is to update coordinates of mobile devices)
             */
            if (OffloadingSetup.mobility)
                postTaskScheduling(scheduling);
        }

        System.out.println("Counters: " + allTasks.size() + ", " + schedulingCounter);

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
}
