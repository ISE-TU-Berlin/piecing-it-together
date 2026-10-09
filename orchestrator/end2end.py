import asyncio
import datetime
import sys
import os
import time
from typing import Optional
import logging
from sma import SustainabilityMeasurementAgent, Config, SMAObserver, SMASession, initialize_logging, __version__ as sma_version # type: ignore


import subprocess
from runtime_enviroment_manager import WasmtimeGRPCEnviromentManager, WasmtimeVanillaEnviromentManager, BaselineEnviromentManager, SystemStressGenerator, WasmtimeFullEnviromentManager, WasmtimeTCPEnviromentManager, SyntheticServiceManager
from experiment_config import ExperimentConfig
logLevel = os.getenv("LOGLEVEL", "INFO").upper()
# already initialized through the module when importing SMA, so we'll redefine with rich logging
initialize_logging(logLevel, logger_class="rich.logging.RichHandler")
logging.basicConfig(format="%(name)s: %(message)s")
log = logging.getLogger("e2e")
    
class End2EndExperiment(SMAObserver):
    """Experiment class for running isolated services benchmarks."""
    
    def __init__(self, experiment_config: ExperimentConfig):
        """Initialize the experiment with configuration parameters."""
        self.experiment = experiment_config
        
        self.type : Optional[str] = None
        self.port : int = 50051

        self.enviroment_managers = {
            "baseline": BaselineEnviromentManager(self.experiment),
            "wasmtime-vanilla": WasmtimeVanillaEnviromentManager(self.experiment),
            "wasmtime-grpc": WasmtimeGRPCEnviromentManager(self.experiment),
            "wasmtime-tcp": WasmtimeTCPEnviromentManager(self.experiment),
        }
        
        self.stressor = SystemStressGenerator(self.experiment)
        self.synthetic_service_manager = SyntheticServiceManager(self.experiment)
    
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
            if type_name in self.enviroment_managers:
                self.enviroment_managers[type_name].prepare()
            else:
                raise ValueError(f"Unknown workload type: {type_name}")
        
        self.stressor.prepare()
        self.synthetic_service_manager.prepare()
        
        log.info("Experiment environment prepared successfully.")
    
    def on_start(self, type: str) -> int:
        """Start a workload of the specified type."""
        log.info(f"Starting workload of type: {type}")
        if type in self.enviroment_managers:
            self.port = self.enviroment_managers[type].start()
        else:
            raise ValueError(f"Unknown workload type: {type}")
        
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

    def workload(self) -> dict:
        """Run a workload with the specified configuration."""
        
        raise NotImplementedError("Workload method not implemented yet.")
            
        return {"workload_type": self.type }

    def onRunStart(self):
        if self.type is not None:
            self.on_start(self.type)
        else:
            raise ValueError("Workload type not set before run start.")
    
    def onRunEnd(self, run):
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
    
    def main(self):
        """Main experiment execution method."""
        log.info("Loading SMA configuration...")


        config = Config.from_file("end2end_sma_config.yaml")
        log.info(f"Creating Agent SMA {sma_version} with config: {config}")
        sma = SustainabilityMeasurementAgent(config,meta= SMASession(
            name="end2end",
            extras=ExperimentConfig.to_dict(self.experiment)
        ))
        
        log.info("Preparing Experiment Environment...")
        self.prepare()
        
        log.info("Connecting SMA ...")
        sma.connect()
        sma.register_sma_observer(self)
        
        with sma.start_session():
            
            log.info("Starting experiment runs...")
            for service_type in self.experiment.types:
                service_type = service_type.name
                for wl in self.experiment.workload_set:   
                    name = wl.name
                    stress = wl.systemStressor # default to "none" if not specified in workload config
                    workload_profile = self.experiment.find_profile(wl.profile)
                    
                    if workload_profile is None:
                        log.warning(f"Workload config for {wl.profile} not found, skipping this workload.")
                        continue
                    
                    spawn_rate = workload_profile.spawn_rate
                    max_users = workload_profile.users
                    if spawn_rate is None or max_users is None:
                        log.warning(f"Invalid workload config for {wl.profile}, missing 'spawn_rate' or 'users', skipping this workload.")
                        continue
                    
                    log.info(f"Running workload '{name}' with {max_users} users and spawn rate {spawn_rate} for service type {service_type} and stress level {stress}")
                    
                    for i in range(self.experiment.runs):
                        self.type = service_type
                        # overwriting module config
                        try:
                            self.stressor.start(level=stress)
                        except Exception as e:
                            log.error(f"Error starting stressor: {e}")
                        
                        try:
                            sma.run(users=max_users, spawn_rate=spawn_rate, run_time=f"{self.experiment.workload_duration}s", runtime=service_type,nodeIP=self.experiment.build_ssh_hostname, nodeSelector=[self.experiment.node_selector],stress_level=stress, workload_name=name)
                        except Exception as e:
                            log.error(f"Error during workload run: {e}")
                            self.onRunEnd(None) # Ensure we stop the workload if an error occurs
                        finally:
                            self.stressor.stop()
                        time.sleep(30) # Wait between runs


if __name__ == "__main__":
    config = ExperimentConfig.from_yaml("common_experiment_config.yaml", "end2end_experiment_config.yaml", "enviroment.yaml")
    log.info(f"Experiment configuration loaded: {config}")
    experiment = End2EndExperiment(config)
    experiment.main()
