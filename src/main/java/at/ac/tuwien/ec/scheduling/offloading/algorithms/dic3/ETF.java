package at.ac.tuwien.ec.scheduling.offloading.algorithms.dic3;

import java.util.ArrayList;
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
        // setRank(this.currentApp,this.currentInfrastructure);
    }

    public ETF(Tuple2<MobileApplication, MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
        // setRank(this.currentApp,this.currentInfrastructure);
    }

    @Override
    public ArrayList<? extends OffloadScheduling> findScheduling() {
        // execution time
        double start = System.nanoTime();

        ArrayList<MobileSoftwareComponent> taskList = new ArrayList<>();
        // ETF output: starting time, finishing time and processor for each task
        ArrayList<OffloadScheduling> schedulings = new ArrayList<OffloadScheduling>();

        // ???
        DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> deps = this.getMobileApplication()
                .getTaskDependencies();

        System.out.println(deps);

        // nodes with 0 incoming edges - ready to execute
        ArrayList<MobileSoftwareComponent> availableTasks = this.getMobileApplication().readyTasks();
        // PriorityQueue<MobileSoftwareComponent> availableTasks = new PriorityQueue<MobileSoftwareComponent>(new NodeRankComparator());

        // free processors
        ArrayList<ComputationalNode> availableNodes = this.getInfrastructure().getAllNodes();

        double currentMoment = 0;
        double nextMoment = Double.MAX_VALUE;

        // while scheduledTasks
        while (!availableTasks.isEmpty()) {

        }

        OffloadScheduling scheduling = new OffloadScheduling();
        ComputationalNode target;

        for (MobileSoftwareComponent currTask : taskList) {

            target = null;

            double tMin = Double.MAX_VALUE; // Minimum execution time for next task

            if (!currTask.isOffloadable()) {
                // If task is not offloadable, deploy it in the mobile device (if enough
                // resources are available)
                ComputationalNode localDevice = (ComputationalNode) currentInfrastructure
                        .getNodeById(currTask.getUserId());
                if (isValid(scheduling, currTask, localDevice)) {
                    target = localDevice;
                }
            } else {
                // Check for all available Cloud/Edge nodes
                for (ComputationalNode cn : currentInfrastructure.getAllNodes())
                    if (currTask.getRuntimeOnNode(cn, currentInfrastructure) < tMin
                            && isValid(scheduling, currTask, cn)) {
                        // Earliest Finish Time EFT = wij + EST
                        tMin = cn.getESTforTask(currTask) + currTask.getRuntimeOnNode(cn, currentInfrastructure);
                        target = cn;

                    }
                /*
                 * We need this check, because there are cases where, even if the task is
                 * offloadable, local execution is the best option
                 */
                ComputationalNode localDevice = (ComputationalNode) currentInfrastructure
                        .getNodeById(currTask.getUserId());
                if (currTask.getRuntimeOnNode(localDevice, currentInfrastructure) < tMin
                        && isValid(scheduling, currTask, localDevice)) {
                    // Earliest Finish Time EFT = wij + EST
                    tMin = localDevice.getESTforTask(currTask)
                            + currTask.getRuntimeOnNode(localDevice, currentInfrastructure);
                    target = localDevice;
                }
            }
            if (target != null) {
                deploy(scheduling, currTask, target);
            }
            // scheduledNodes.add(currTask);
            // tasks.remove(currTask);
            // } else if (!scheduledNodes.isEmpty()) {
            // MobileSoftwareComponent terminated = scheduledNodes.remove();
            // ((ComputationalNode) scheduling.get(terminated)).undeploy(terminated);
            // }

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
}
