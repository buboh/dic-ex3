package at.ac.tuwien.ec.scheduling.offloading.algorithms.dic3;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.jgrapht.graph.DirectedAcyclicGraph;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import scala.Tuple2;

/**
 * OffloadScheduler class that randomly schedules tasks to processors
 * 
 * Taken from the DIC Ex-3 SLEIPNIR Tutorial
 */

public class RandomTutorial extends OffloadScheduler {

    /**
     *
     * @param A MobileApplication property from SimIteration
     * @param I MobileCloudInfrastructure property from SimIteration Constructors
     *          set the parameters and calls setRank() to nodes' ranks
     */

    public RandomTutorial(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
        // setRank(this.currentApp,this.currentInfrastructure);
    }

    public RandomTutorial(Tuple2<MobileApplication, MobileCloudInfrastructure> t) {
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

        DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> deps = this.getMobileApplication()
                .getTaskDependencies();

        // nodes with 0 incoming edges - ready to execute
        ArrayList<MobileSoftwareComponent> ready = this.getMobileApplication().readyTasks();
        System.out.println(ready);

        Iterator<MobileSoftwareComponent> it = deps.iterator();
        while (it.hasNext()) {
            MobileSoftwareComponent vertex = it.next();
            taskList.add(vertex);
        }

        OffloadScheduling scheduling = new OffloadScheduling();
        ComputationalNode target;

        for (MobileSoftwareComponent currTask : taskList) {

            target = null;

            if (!currTask.isOffloadable()) {
                ComputationalNode node = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
                if (isValid(scheduling, currTask, node)) {
                    target = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
                }
            } else {
                UniformIntegerDistribution uid = new UniformIntegerDistribution(0,
                        currentInfrastructure.getAllNodes().size() - 1);
                ComputationalNode tmpTarget = (ComputationalNode) currentInfrastructure.getAllNodes().toArray()[uid
                        .sample()];
                if (isValid(scheduling, currTask, tmpTarget)) {
                    target = tmpTarget;
                }
            }
            if (target != null) {
                deploy(scheduling, currTask, target);
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
}
