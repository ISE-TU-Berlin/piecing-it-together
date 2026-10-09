import subprocess
import os
import shutil
import threading
import time
import uuid
import yaml # type: ignore
import uuid

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
import logging 
from experiment_config import ExperimentConfig  
from kubernetes import client, config, watch # type: ignore
from sma import Triggerable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class EnviromentManager(ABC):
    """Abstract base class for workload managers."""
    def __init__(self, cnf: ExperimentConfig):
        self.config = cnf

    @abstractmethod
    def prepare(self):
        """Prepare the environment for the workload."""
        pass

    @abstractmethod
    def start(self, **kwargs) -> int:
        """Start the workload and return the port."""
        pass

    @abstractmethod
    def stop(self, **kwargs):
        """Stop the workload."""
        pass

class SystemStressGenerator(EnviromentManager):
    
    STRESS_LEVELS = {
        "low": {
            "cpu_load": 5,
            "vm_bytes": "5%",
            "hdd_bytes": "1%",
        },
        "medium": {
            "cpu_load": 15,
            "vm_bytes": "15%",
            "hdd_bytes": "5%",
        },
        "high": {
            "cpu_load": 25,
            "vm_bytes": "25%",
            "hdd_bytes": "10%",
        },
    }

    def render_stress_job(self, level: str) -> Optional[Tuple[str, str]]:
        """
        level: "0", "low", "medium", or "high"
        timeout_seconds: runtime in seconds

        Returns:
        - YAML string for the Job
        - Job name
        - None if level == "0" (meaning: do not deploy)
        """
        if str(level) == "0" or level == "none":
            return None

        if level not in self.STRESS_LEVELS:
            raise ValueError("level must be one of: 0, low, medium, high")

        cfg = self.STRESS_LEVELS[level]
        run_id = str(uuid.uuid4())[:8]

        args = [
            "--cpu", "-1",
            "--cpu-method", "all",
            "--cpu-load", str(cfg["cpu_load"]),
            "--cpu-load-slice", "50",
            "--vm", "1",
            "--vm-bytes", cfg["vm_bytes"],
            "--vm-method", "flip",
            "--hdd", "1",
            "--hdd-bytes", cfg["hdd_bytes"],
            "--metrics-brief",
        ]

        job_name = f"stress-ng-{level}-{run_id}"
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": "default",
            },
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "stress-ng",
                            "stress-level": level,
                            "run-id": run_id,
                        }
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "nodeSelector": {
                            "role": "sut",
                        },
                        "terminationGracePeriodSeconds": 5,
                        "containers": [
                            {
                                "name": "stress-ng",
                                "image": "ghcr.io/colinianking/stress-ng:latest",
                                "imagePullPolicy": "IfNotPresent",
                                "args": args,
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": False,
                                },
                            }
                        ],
                    },
                },
            },
        }

        return yaml.safe_dump(job, sort_keys=False), job_name
    
    
    def __init__(self, cnf: ExperimentConfig):
        super().__init__(cnf)
        self.current_job_name = None
    
    def prepare(self):
        return super().prepare()
    
    def cleanup(self):
        kubectl_delete = subprocess.run(["kubectl", "delete", "job", "-l", "app=stress-ng", "--ignore-not-found"], check=False)
        if kubectl_delete.returncode != 0:
            logger.warning("Failed to clean up existing stress jobs. Error: %s \n %s", kubectl_delete.stderr, kubectl_delete.stdout)
    
    def start(self, **kwargs) -> int:
        self.cleanup()  # Ensure no existing stress jobs are running before starting a new one
        job = self.render_stress_job(kwargs.get("level", "none"))
        if job is None:
            self.current_job_name = None
            logger.info("No stress job to deploy for level '0' or 'none'.")
            return -1
        
        job_yaml, job_name = job
        apply_cmd = ["kubectl", "apply", "-f", "-"]
        proc = subprocess.run(apply_cmd, input=job_yaml.encode(), check=True)
        assert proc.returncode == 0, "Failed to apply stress job"
        logger.info(f"Deployed stress job '{job_name}' with level '{kwargs.get('level')}'.")
        self.current_job_name = job_name
        
        return 0

    def stop(self, **kwargs):
        if self.current_job_name is None:
            logger.info("No stress job to stop.")
            return
        delete_cmd = ["kubectl", "delete", "job", self.current_job_name, "--ignore-not-found"]
        proc = subprocess.run(delete_cmd, check=True)
        if proc.returncode != 0:
            logger.warning(f"Failed to delete stress job '{self.current_job_name}'. It may have already completed. Error: {proc.stderr} \n {proc.stdout}")
        else:
            logger.info(f"Deleted stress job '{self.current_job_name}'.")
        self.current_job_name = None
        
class SyntheticServiceManager(EnviromentManager):
    """
        This em is not used to deploy a synthetic service, but to drive the stress level.
    """
    
    STRESS_LEVELS = {
        "low": {
            "CPU_LEVEL": "1",
            "MEMORY_LEVEL": "1",
            "IO_LEVEL": "1",
            "NETWORK_LEVEL": "1",
            "DB_LEVEL": "1"
        },
        "medium": {
            "CPU_LEVEL": "2",
            "MEMORY_LEVEL": "2",
            "IO_LEVEL": "2",
            "NETWORK_LEVEL": "2",
            "DB_LEVEL": "2"
        },
        "high": {
            "CPU_LEVEL": "2",
            "MEMORY_LEVEL": "2",
            "IO_LEVEL": "2",
            "NETWORK_LEVEL": "2",
            "DB_LEVEL": "2"
        },
        "compute": {
            "CPU_LEVEL": "2",
            "MEMORY_LEVEL": "2",
            "IO_LEVEL": "0",
            "NETWORK_LEVEL": "0",
            "DB_LEVEL": "0"
        },
        "network": {
            "CPU_LEVEL": "0",
            "MEMORY_LEVEL": "0",
            "IO_LEVEL": "0",
            "NETWORK_LEVEL": "2",
            "DB_LEVEL": "2"
        },
        "data":
        {
            "CPU_LEVEL": "0",
            "MEMORY_LEVEL": "0",
            "IO_LEVEL": "2",
            "NETWORK_LEVEL": "0",
            "DB_LEVEL": "0"
        },
        "none": {
            "CPU_LEVEL": "0",
            "MEMORY_LEVEL": "0",
            "IO_LEVEL": "0",
            "NETWORK_LEVEL": "0",
            "DB_LEVEL": "0"
        }
    }
    
    def __init__(self, cnf: ExperimentConfig):
        super().__init__(cnf)
        config.load_kube_config()
        self.client = client.CoreV1Api()
        self.deployment_client = client.AppsV1Api()
    
    def prepare(self):
        return super().prepare()
    
    def restart_rollout(self, namespace, deployment_name):
        patch = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": str(uuid.uuid4())
                        }
                    }
                }
            }
        }
        result = self.deployment_client.patch_namespaced_deployment(name=deployment_name, namespace=namespace, body=patch)
        
        if result:
            logger.info(f"Restarted rollout for deployment '{deployment_name}' in namespace '{namespace}'.")
        else:
            logger.warning(f"Failed to restart rollout for deployment '{deployment_name}' in namespace '{namespace}'.")
        
        
    def start(self, **kwargs) -> int:
        stress_level = kwargs.get("level", "none")
        if stress_level not in self.STRESS_LEVELS:
            raise ValueError(f"Invalid stress level '{stress_level}'. Must be one of: {list(self.STRESS_LEVELS.keys())} or 'none'.")
        
        
        result = self.client.patch_namespaced_config_map(
            name="syntheticservice-stress-level",
            namespace=self.config.namespace,
            body={"data": self.STRESS_LEVELS[stress_level]},
        )
        if result:
            logger.info(f"Updated synthetic service config map with stress level '{stress_level}'.")
        
        #roolout the deployment to ensure the new config is picked up
        self.restart_rollout(self.config.namespace, "syntheticservice")
        
        return 0

    def stop(self, **kwargs):
        result = self.client.patch_namespaced_config_map(
            name="syntheticservice-stress-level",
            namespace=self.config.namespace,
            body={"data": self.STRESS_LEVELS["none"]},
        )
        if result:
            logger.info("Reset synthetic service config map to 'none' stress level.")
        
class SyntheticOpsManager(EnviromentManager, Triggerable):
    def __init__(self, cnf: ExperimentConfig):
        super().__init__(cnf)
        config.load_kube_config()
        self.client = client.AppsV1Api()
    
    def prepare(self):
        return super().prepare()
    
    def trigger(self, cancel: threading.Event, **kwargs) -> Optional[Dict[str, Any]]:
        scale = kwargs.get("scale", [])
        settelment_delay = kwargs.get("settlement_delay", 30)
        if scale is None or not isinstance(scale, list) or not all(isinstance(s, int) and s >= 0 for s in scale):
            raise ValueError("Scale parameter is required and must be a non-negative integer.")
        
        datetime_before = time.time()
        scaleing_steps = {}
        time.sleep(settelment_delay)  # Wait for the system to settle before starting the scaling operations
        for idx,s in enumerate(scale):
            start_time = time.time() - datetime_before
            self.start(scale=s)
            success = self.wait_until_ready(targetScale=s, cancel=cancel)
            end_time = time.time() - datetime_before
            scaleing_steps[idx] = {"scale": s, "start": start_time, "end": end_time, "success": success}
            time.sleep(settelment_delay)  # Wait for the system to settle before the next scaling operation
            if cancel.is_set():
                logger.info("Scaling operation cancelled. Exiting trigger.")
                break
        
        return {"scaling_steps": scaleing_steps}
        
    
    def start(self, **kwargs) -> int:
        scale = kwargs.get("scale", 0)
        namespace = self.config.namespace
        target_deployment = "syntheticservice"
        
        result = self.client.patch_namespaced_deployment_scale(
            name=target_deployment,
            namespace=namespace,
            body={"spec": {"replicas": scale}},
        )
        
        logger.info(f"Scaled synthetic service to {scale} replicas.")

        return 0

    def wait_until_ready(self, targetScale: int, cancel: threading.Event) -> bool:
        """
            Wait until the synthetic service has scaled to the target number of replicas. Returns True if successful, False if timeout is reached.
            Checks in intervals and respects the cancel event to allow for early termination if needed.
            Runs in parallel until scussess, timeout or cancel event is set.
            
        """
        
        namespace = self.config.namespace
        target_deployment = "syntheticservice"
        
        def check_ready(scale: int) -> bool:
            w = watch.Watch()
            success = False
            for event in w.stream(self.client.list_namespaced_deployment, namespace=namespace, timeout_seconds=30):
                deployment = event['object'] # type: ignore
                if deployment.metadata.name == target_deployment: # type: ignore
                    ready_replicas = deployment.status.ready_replicas or 0 # type: ignore
                    logger.info(f"Waiting for synthetic service to scale: {ready_replicas}/{targetScale} ready")
                    if ready_replicas == targetScale:
                        logger.info("Synthetic service scaled successfully.")
                        success = True
                        w.stop()
                        break
            if not success:
                logger.warning("Timeout while waiting for synthetic service to scale.")
            return success

        while not cancel.is_set():
            if check_ready(targetScale):
                return True
            time.sleep(1)  # Wait before checking again
            deps = self.client.list_namespaced_deployment(namespace=namespace)  # Trigger watch to update
            for dep in deps.items:
                if dep.metadata.name == target_deployment:
                    logger.info(f"Current replicas: {dep.status.ready_replicas}/{targetScale}")
                    if  dep.status.ready_replicas and dep.status.ready_replicas == targetScale:
                        logger.info("Synthetic service scaled successfully (polled).")
                        return True
                        
        return False  # Return false if cancel event is set before scaling is successful
        

    def stop(self, **kwargs):
        namespace = self.config.namespace
        target_deployment = "syntheticservice"
        
        result = self.client.patch_namespaced_deployment_scale(
            name=target_deployment,
            namespace=namespace,
            body={"spec": {"replicas": 0}},
        )
        if result:
            logger.info("Scaled synthetic service to 0 replicas.")


def compile_wasm_service(wasm_service_path: str, output_path: str, fname: str):
    wasm_compile = subprocess.run(["cargo", "build", "--lib", "--target", "wasm32-wasip2", "--release"], cwd=wasm_service_path, check=True)
    assert wasm_compile.returncode == 0, "WASM compilation failed"
    wasm_binary = os.path.join(wasm_service_path, "target", "wasm32-wasip2", "release", fname)
    assert os.path.isfile(wasm_binary), "WASM binary not found after compilation"
    shutil.copy(wasm_binary, output_path)
    logger.info("WASM service compiled successfully.")

def build_wasi_image_on_targets(wasm_service_path: str, image_name: str,  build_host_ssh_name: str, *stu_host_ssh_name: str, tag: str = "latest"):
    full_path = os.path.join(wasm_service_path,"src", image_name)
    compile_wasm_service(full_path, f"{image_name}.wasm", f"{image_name}.wasm")
    
    copy_wasm = subprocess.run(["scp", f"{image_name}.wasm", f"{build_host_ssh_name}:~/{image_name}.wasm"], check=True)
    assert copy_wasm.returncode == 0, "SCP of wasm binary to target failed"
    logger.info("WASM binary copied to target successfully.")

    build_wasi = subprocess.run(["ssh", build_host_ssh_name, "oci-tar-builder", "--name", f"{image_name}", "--repo", "ghcr.io/containerd/runwasi", "--tag", tag, "--module", f"{image_name}.wasm", "-o", f"{image_name}.tar"], check=True)
    
    assert build_wasi.returncode == 0, "WASI OCI tar creation on target failed"
    for target in stu_host_ssh_name:
        copy_wasi = subprocess.run(["scp", f"{build_host_ssh_name}:{image_name}.tar", f"{target}:~/{image_name}.tar"], check=True)    
        assert copy_wasi.returncode == 0, f"SCP of WASI OCI tar to target {target} failed"
        load_wasi = subprocess.run(["ssh", target,"sudo", "ctr","-n","k8s.io","image","import", "--all-platforms", f"{image_name}.tar"], check=True)
        assert load_wasi.returncode == 0, "Loading WASI OCI tar on target failed"
        
    logger.info(f"Build and pushed ghcr.io/containerd/runwasi/{image_name}:{tag}")

class SkaffoldEnviromentManager(EnviromentManager):
    
    def __init__(self, cnf: ExperimentConfig, profiles: List[str]):
        super().__init__(cnf)
        self.profiles = profiles
    
    """Workload manager for Skaffold."""
    def prepare(self):
        
        skaffold_build = subprocess.run(["skaffold", "--default-repo",self.config.image_reposetory, "build"], check=True, cwd=self.config.skaffold_path)
        assert skaffold_build.returncode == 0, "Skaffold build failed"
        logger.debug("Skaffold environment prepared successfully.")

    def start(self, **kwargs) -> int:
        skaffold_run = subprocess.run(["skaffold", "run","-n",self.config.namespace, "--default-repo",self.config.image_reposetory,"-p", ",".join(self.profiles)], check=True, cwd=self.config.skaffold_path)
        assert skaffold_run.returncode == 0, "Skaffold run failed"
        logger.debug("Skaffold workload started successfully.")
        return 30051

    def stop(self, **kwargs):
        skaffold_delete = subprocess.run(["skaffold", "delete", "-n", self.config.namespace, "-p", ",".join(self.profiles)], check=False, cwd=self.config.skaffold_path)
        if skaffold_delete.returncode != 0:
            logger.warning("Skaffold delete failed, attempting to continue with cleanup. Error: %s \n %s", skaffold_delete.stderr, skaffold_delete.stdout)
        assert skaffold_delete.returncode == 0, "Skaffold delete failed"
        logger.debug("Skaffold workload stopped successfully.")
     
class BaselineEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for baseline deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["baseline"])

class WasmtimeVanillaEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for Wasip deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["wasi-vanilla"])
        
    def prepare(self):
        super().prepare()
        build_wasi_image_on_targets(self.config.wasm_service_path, "shippingserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")

class WasmtimeGRPCEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for Wasip deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["wasi-grpc"])
        
    def prepare(self):
        super().prepare()
        build_wasi_image_on_targets(self.config.wasm_service_path, "shippingserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")
        build_wasi_image_on_targets(self.config.wasm_service_path, "recommendationserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")

class WasmtimeTCPEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for Wasip deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["wasi-tcp"])
        
    def prepare(self):
        super().prepare()
        build_wasi_image_on_targets(self.config.wasm_service_path, "shippingserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")
        build_wasi_image_on_targets(self.config.wasm_service_path, "recommendationserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")
        build_wasi_image_on_targets(self.config.wasm_service_path, "cartserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")

class WasmtimeFullEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for Wasip deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["wasi-all"])
        
    def prepare(self):
        super().prepare()
        build_wasi_image_on_targets(self.config.wasm_service_path, "shippingserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")
        build_wasi_image_on_targets(self.config.wasm_service_path, "recommendationserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")
        build_wasi_image_on_targets(self.config.wasm_service_path, "cartserwasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")
        build_wasi_image_on_targets(self.config.wasm_service_path, "syntheticservicewasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")


class SyntheticBaselineEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for synthetic baseline deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["synthetic-baseline"])
    
    def prepare(self):
        super().prepare()
        build_wasi_image_on_targets(self.config.wasm_service_path, "syntheticservicewasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")

class SyntheticWasmEnviromentManager(SkaffoldEnviromentManager):
    """Workload manager for synthetic Wasm deployments."""
    def __init__(self, cnf):
        super().__init__(cnf, profiles=["synthetic-wasm"])
        
    def prepare(self):
        super().prepare()
        build_wasi_image_on_targets(self.config.wasm_service_path, "syntheticservicewasm", self.config.build_ssh_hostname, *self.config.sut_ssh_hostnames, tag="latest")

if __name__ == "__main__":
    cnf = ExperimentConfig.from_yaml("end2end_experiment_config.yaml", "environment.yaml","common_experiment_config.yaml")
    skf = WasmtimeFullEnviromentManager(cnf)
    skf.prepare()