package at.ac.tuwien.ec.scheduling.offloading.algorithms.mcp;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.offloading.algorithms.heftbased.utils.NodeRankComparator;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import scala.Tuple2;

/**
 * OffloadScheduler class that implements the
 * Heterogeneous Earliest-Finish-Time (HEFT) algorithm
 * , a static scheduling heuristic, for efficient application scheduling
 *
 * H. Topcuoglu, S. Hariri and Min-You Wu,
 * "Performance-effective and low-complexity task scheduling for heterogeneous computing,"
 * in IEEE Transactions on Parallel and Distributed Systems, vol. 13, no. 3, pp. 260-274, March 2002, doi: 10.1109/71.993206.
 */

public class MCPScheduler extends OffloadScheduler {

    private final Map<MobileSoftwareComponent, Double> alapTimes = new HashMap<>();
    private final Map<MobileSoftwareComponent, List<Double>> alapLists = new HashMap<>();
    private double minAlap = Double.MAX_VALUE;


    /**
     *
     * @param A MobileApplication property from  SimIteration
     * @param I MobileCloudInfrastructure property from  SimIteration
     * Constructors set the parameters and calls setRank() to nodes' ranks
     */

    public MCPScheduler(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
        calculatePriorities(this.currentApp);
    }

    public MCPScheduler(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
        calculatePriorities(this.currentApp);
    }

    /**
     * Processor selection phase:
     * select the tasks in order of their priorities and schedule them on its "best" processor,
     * which minimizes task's executuion! time
     * @return
     */
    @Override
    public ArrayList<? extends OffloadScheduling> findScheduling() {
        double start = System.nanoTime();

        Set<MobileSoftwareComponent> readyTasks = new HashSet<>();
        readyTasks.addAll(Arrays.asList(this.getMobileApplication().readyTasks().toArray(new MobileSoftwareComponent[0])));

        OffloadScheduling scheduling = new OffloadScheduling();
        ArrayList<OffloadScheduling> deployments = new ArrayList<OffloadScheduling>();

        while (!readyTasks.isEmpty()) {
            MobileSoftwareComponent chosen = this.choose(readyTasks.toArray(new MobileSoftwareComponent[readyTasks.size()]));
            readyTasks.remove(chosen);

            double tMin = Double.MAX_VALUE;
            ComputationalNode target = null;

            if(!chosen.isOffloadable()) {
                // If task is not offloadable, deploy it in the mobile device (if enough resources are available)
                if(isValid(scheduling,chosen,(ComputationalNode) currentInfrastructure.getNodeById(chosen.getUserId())))
                    target = (ComputationalNode) currentInfrastructure.getNodeById(chosen.getUserId());

            }
            else {
                for (ComputationalNode cn : currentInfrastructure.getAllNodes()) {
                    if (cn.getESTforTask(chosen) < tMin &&
                            isValid(scheduling, chosen, cn)) {
                        tMin = cn.getESTforTask(chosen); // Earliest Start Time
                        target = cn;
                    }
                }
            }

            if(target != null)
            {
                deploy(scheduling,chosen,target);
            }

            /*
             * if simulation considers mobility, perform post-scheduling operations
             * (default is to update coordinates of mobile devices)
             */
            if(OffloadingSetup.mobility)
                postTaskScheduling(scheduling);
        }

        double end = System.nanoTime();
        scheduling.setExecutionTime(end-start);
        deployments.add(scheduling);
        return deployments;
    }


    public MobileSoftwareComponent choose(final MobileSoftwareComponent... readyTasks) {
        Objects.requireNonNull(readyTasks);
        if (readyTasks.length == 0) {
            throw new IllegalArgumentException("readyTasks should not be of length 0");
        } else if (readyTasks.length == 1) {
            return readyTasks[0];
        }

        final List<MobileSoftwareComponent> readyAsList = Arrays.asList(readyTasks);

        List<MobileSoftwareComponent> sorted = alapLists.entrySet().stream()
                .filter(entry -> readyAsList.contains(entry.getKey()))
                .sorted((x, y) ->
                        x.getValue().toString().compareTo(y.getValue().toString()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        return sorted.get(0);
    }



    public void calculatePriorities(MobileApplication A) {
        calculateAlap(A.taskDependencies);
        A.taskDependencies.vertexSet().forEach(exe -> createALAPListForAllNodes(A.taskDependencies, exe));
    }

    private void calculateAlap(final DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> graph) {
        Set<MobileSoftwareComponent> allVertices = graph.vertexSet();
        allVertices.stream()
                .filter(task -> graph.inDegreeOf(task) == 0)
                .forEach(task -> alapFromOneNode(graph, task));
        double executionTime = -this.minAlap;
        allVertices.forEach(exe -> this.alapTimes.put(exe, executionTime + this.alapTimes.get(exe)));
    }

    private double alapFromOneNode(final DirectedAcyclicGraph<MobileSoftwareComponent,ComponentLink> graph, final MobileSoftwareComponent current) {
        if (this.alapTimes.containsKey(current)) {
            return this.alapTimes.get(current);
        }

        double w_cmp = 0.0; // average execution time of task on each processor / node of this component

        int numberOfNodes = this.currentInfrastructure.getAllNodes().size() + 1;
        for(ComputationalNode cn : this.currentInfrastructure.getAllNodes())
            w_cmp += current.getLocalRuntimeOnNode(cn, this.currentInfrastructure);

        w_cmp = w_cmp / numberOfNodes;

        if (graph.outDegreeOf(current) == 0) {
            double tmp = -w_cmp;
            this.alapTimes.put(current, tmp);
            this.minAlap = Math.min(tmp, this.minAlap);
            return tmp;
        } else {
            Set<ComponentLink> edges = graph.outgoingEdgesOf(current);
            Stream<MobileSoftwareComponent> neighbours = edges.stream().map(graph::getEdgeTarget);
            double minStartTimeOfNeighbours = neighbours.map(next -> alapFromOneNode(graph, next)).min(Double::compare).get();
            double minStartTimeOfCurrent = -w_cmp;
            this.minAlap = Math.min(minStartTimeOfCurrent, this.minAlap);
            this.alapTimes.put(current, minStartTimeOfCurrent);
            return minStartTimeOfCurrent;
        }

    }

    private List<Double> createALAPListForAllNodes(final DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> graph, final MobileSoftwareComponent current) {
        if (alapLists.containsKey(current)) {
            return alapLists.get(current);
        }

        if (graph.outDegreeOf(current) == 0) {
            List<Double> result = Collections.singletonList(this.alapTimes.get(current));
            alapLists.put(current, result);
            return result;
        } else {
            Set<ComponentLink> edges = graph.outgoingEdgesOf(current);
            Stream<MobileSoftwareComponent> neighbours = edges.stream().map(graph::getEdgeTarget);
            List<Double> result = new LinkedList<>();
            neighbours.map(next -> createALAPListForAllNodes(graph, next))
                    .collect(Collectors.toList())
                    .forEach(result::addAll);
            result.add(this.alapTimes.get(current));
            result.sort(Double::compare);
            alapLists.put(current, result);

            return result;
        }
    }
}
