import asyncio
import datetime
import sys
import os
import threading
import time
from typing import Optional
import logging
from sma import Any, Dict, Event, List, SustainabilityMeasurementAgent, Report, Config, SMAObserver, SMASession, Triggerable, initialize_logging, __version__ as sma_version # type: ignore


import subprocess
from runtime_enviroment_manager import SyntheticBaselineEnviromentManager, SyntheticWasmEnviromentManager, SyntheticOpsManager, SyntheticServiceManager, SystemStressGenerator
from stress_test import StressTestConfig, SMAStressTester, StressTester

class JoinedSMATrigger(Triggerable):
    def __init__(self, treatment: SyntheticOpsManager, workload: SMAStressTester):
        self.treatment = treatment
        self.workload = workload
        
        self.done = None
        self.cancel = None
        
    def stop(self):
        if self.cancel is not None:
            self.cancel.set()  # signal both threads to stop
        if self.done is not None:
            self.done.set()  # ensure any waiting thread is released
            
        
    def trigger(self, cancel: Event, **kwargs) -> Dict[str, Any] | None:
        done = threading.Event()
        self.done = done  # store the event in the instance to be accessed by threads
        self.cancel = cancel  # store the cancel event in the instance to be accessed by threads
        
        results: List[Dict[str, Any] | None] = [None, None]  # to store results from both threads
        def run(idx: int, watch: bool, trigger_fn: Triggerable, cancel: threading.Event):
            results[idx] = trigger_fn.trigger(cancel, **kwargs)
            print(f"Thread {idx} finished with result: {results[idx]}")
            if watch:
                done.set()  # signal the other to stop
        
        # We want the treatment to stop also the worklaod but not the other way around, so we set watch=True for the treatment thread
        treatment_thread = threading.Thread(target=run, args=(0,True, self.treatment, cancel),daemon=True)
        workload_thread = threading.Thread(target=run, args=(1, False, self.workload, cancel),daemon=True)
        
        def watchdog():
            done.wait()  # wait for either thread to finish
            cancel.set()  # signal the other thread to stop
        
        watchdog_thread = threading.Thread(target=watchdog, daemon=True)
        
        treatment_thread.start()
        workload_thread.start()
        watchdog_thread.start()
        
        treatment_thread.join()
        workload_thread.join()
        
        final_result = {}
        if results[0] is not None:
            final_result.update(results[0])
        if results[1] is not None:
            final_result.update(results[1])
            
        return final_result

    def write_workload_results(self, run_directory: str):
        # Implement logic to write workload results to a file or database
        self.workload.write_metrics(run_directory)

from experiment_config import ExperimentConfig
logLevel = os.getenv("LOGLEVEL", "INFO").upper()
# already initialized through the module when importing SMA, so we'll redefine with rich logging
initialize_logging(logLevel, logger_class="rich.logging.RichHandler")
logging.basicConfig(format="%(name)s: %(message)s")
log = logging.getLogger("isolated")
    
class IsolatedExperiment(SMAObserver):
    """Experiment class for running isolated services benchmarks."""
    
    def __init__(self, experiment_config: ExperimentConfig):
        """Initialize the experiment with configuration parameters."""
        self.experiment = experiment_config
        
        self.type : Optional[str] = None
        self.port : int = 50051

        self.enviroment_managers = {
            "baseline": SyntheticBaselineEnviromentManager(self.experiment),
            "wasmtime": SyntheticWasmEnviromentManager(self.experiment),
        }
        
        self.stressor = SystemStressGenerator(self.experiment)
        self.synthetic_service_manager = SyntheticServiceManager(self.experiment)
        self.synthetic_ops_manager = SyntheticOpsManager(self.experiment)
        
        self.trigger : Optional[JoinedSMATrigger] = None
    
    def prepare(self, ):
        """Prepare the experiment environment."""
        
        out = subprocess.check_output(["ssh", self.experiment.build_ssh_hostname, "echo 'test'"])
        assert out.strip() == b'test', "SSH to target host failed"
        assert len(self.experiment.sut_ssh_hostnames) > 0, "No SUT SSH hostnames provided in configuration"
        for target in self.experiment.sut_ssh_hostnames:
            out = subprocess.check_output(["ssh", target, "echo 'test'"])
            assert out.strip() == b'test', f"SSH to target host {target} failed"
        log.info("SSH connectivity to all target hosts verified successfully.")
        # Compile and copy service client

        for type_name in self.experiment.types:
            type_name = type_name.name
            if type_name in self.enviroment_managers.keys():
                self.enviroment_managers[type_name].prepare()
            else:
                raise ValueError(f"Unknown workload type: {type_name}")
        
        self.stressor.prepare()
        self.synthetic_service_manager.prepare()
        self.synthetic_ops_manager.prepare()    
        log.info("Experiment environment prepared successfully.")
    
    def on_start(self, type: str) -> int:
        """Start a workload of the specified type."""
        log.info(f"Starting workload of type: {type}")
        if type in self.enviroment_managers:
            self.port = self.enviroment_managers[type].start()
        else:
            raise ValueError(f"Unknown workload type: {type}")
        
        if self.service_stress_level is not None:
            self.synthetic_service_manager.start(level=self.service_stress_level)
        log.info(f"Workload of type {type} started on port {self.port}")
        
        return self.port

    def on_stop(self, type: str):
        """Stop a workload of the specified type."""
        log.info(f"Stopping workload of type: {type}")
        if type in self.enviroment_managers:
            self.enviroment_managers[type].stop()
        else:
            raise ValueError(f"Unknown workload type: {type}")
        
        self.port = -1
        self.type = None
        
        log.info(f"Workload of type {type} stopped.")

    def onRunStart(self):
        if self.type is not None:
            self.on_start(self.type)
        else:
            raise ValueError("Workload type not set before run start.")
    
    def onRunEnd(self, run=None) -> None:
        if self.type is not None:
            self.on_stop(self.type)
        else:
            raise ValueError("Workload type not set before run end.")
        
    
    def onTaredown(self):
        log.info("Tearing down experiment environment...")
        for manager in self.enviroment_managers.values():
            try:
                manager.stop()
            except Exception as e:
                log.warning(f"Error stopping environment manager: {e}")
        log.info("Experiment environment teardown complete.")
    
    def onReport(self, report=None) -> None: 
        if self.trigger is not None:
            assert isinstance(report, Report), "Expected report to be an instance of sma.Report"
            self.trigger.write_workload_results(report.location) 
    
    def main(self):
        """Main experiment execution method."""
        log.info("Loading SMA configuration...")


        config = Config.from_file("isolation_sma_config.yaml")
        log.info(f"Creating Agent SMA {sma_version} with config: {config}")
        sma = SustainabilityMeasurementAgent(config,meta=SMASession(
            name="isolated-end2end",
            extras=ExperimentConfig.to_dict(self.experiment)
        ))
        
        log.info("Preparing Experiment Environment...")
        self.prepare()
        
        log.info("Connecting SMA ...")
        sma.connect()
        sma.register_sma_observer(self)
        
        with sma.start_session():
            for service_type in self.experiment.types:
                service_type = service_type.name
                for wl in self.experiment.workload_set:
                    name = wl.name
                    stress = wl.systemStressor # default to "none" if not specified in workload config
                    service_stress = wl.serviceStressor # default to "none" if not specified in workload config
                    workload_profile = self.experiment.find_stress_profile(wl.profile)

                    if workload_profile is None:
                        log.warning(f"Workload config for {wl.profile} not found, skipping this workload.")
                        continue

                    stress_tester : SMAStressTester = SMAStressTester.make_tester("TODO", StressTestConfig(
                        host=f"{self.experiment.build_ssh_hostname}:30055", #XXX: magic number
                        total_requests=workload_profile.total_requests,
                        parallel_connections=workload_profile.parallel_connections,
                        requests_per_connection=workload_profile.requests_per_connection,
                        timeout=workload_profile.timeout,
                        log_responses=True,
                    ))

                    self.trigger = JoinedSMATrigger(self.synthetic_ops_manager, stress_tester)

                    for _ in range(self.experiment.runs):
                        self.type = service_type
                        self.service_stress_level = service_stress
                        # overwriting module config
                        scale = wl.scale or []
                        try:
                            self.stressor.start(level=stress)
                        except Exception as e:
                            log.error(f"Error starting stressor: {e}")

                        try:
                            sma.run(self.trigger, scale=scale,stress_level=stress, workload_name=name, service_stress_level=service_stress, treatment=service_type)
                        except Exception as e:
                            log.error(f"Error during workload run: {e}")
                            self.onRunEnd(None) # Ensure we stop the workload if an error occurs
                        finally:
                            self.trigger.stop()  # Ensure the trigger is stopped after each run
                            self.stressor.stop()
                        time.sleep(30) # Wait between runs

if __name__ == "__main__":
    config = ExperimentConfig.from_yaml("common_experiment_config.yaml", "isolation_experiment_config.yaml", "enviroment.yaml")
    log.info(f"Experiment configuration loaded: {config}")
    experiment = IsolatedExperiment(config)
    experiment.main()
