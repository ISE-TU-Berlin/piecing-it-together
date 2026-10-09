from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
import yaml

@dataclass
class ExperimentTypes:
    name: str
    description: str

@dataclass
class WorkloadProfile:
    name: str
    users: int
    spawn_rate: int
    
@dataclass
class WorkloadSet:
    name: str
    profile: str
    serviceStressor: Optional[str] = None
    systemStressor: Optional[str] = "none"
    scale: Optional[List[int]] = None

@dataclass
class StressWorkloadProfile:
    name: str
    total_requests: int = 1000
    parallel_connections: int = 10
    requests_per_connection: int = 10
    timeout: float = 5.0

@dataclass
class ExperimentConfig:
    # Local Enviroment parameters
    skaffold_path: str
    wasm_service_path: str
    
    # Remote Enviroment parameters
    build_ssh_hostname: str
    sut_platform: str
    sut_ssh_hostnames: List[str]
    
    # Kubernetes parameters
    namespace :str
    node_selector : str
    image_reposetory: str
    
    # Experiment parameters
    types: List[ExperimentTypes]
    workload_duration: int
    workload_profiles: List[WorkloadProfile|StressWorkloadProfile]
    workload_set: List[WorkloadSet] 
    runs: int 

    @staticmethod
    def _parse_types(raw_types: List[dict] | None) -> List[ExperimentTypes]:
        if not raw_types:
            return []
        return [ExperimentTypes(**item) for item in raw_types]

    @staticmethod
    def _parse_workload_profiles(
        raw_profiles: List[dict] | None,
    ) -> List[WorkloadProfile | StressWorkloadProfile]:
        if not raw_profiles:
            return []

        parsed_profiles: List[WorkloadProfile | StressWorkloadProfile] = []
        for item in raw_profiles:
            # End-to-end profiles use users/spawn_rate, isolation profiles use request-based fields.
            if "users" in item or "spawn_rate" in item:
                parsed_profiles.append(WorkloadProfile(**item))
            else:
                parsed_profiles.append(StressWorkloadProfile(**item))
        return parsed_profiles

    @staticmethod
    def _parse_workload_set(raw_workload_set: List[dict] | None) -> List[WorkloadSet]:
        if not raw_workload_set:
            return []
        return [WorkloadSet(**item) for item in raw_workload_set]

    def find_profile(self, name: str) -> Optional[WorkloadProfile]:
        for wl in self.workload_profiles:
            if wl.name == name and isinstance(wl, WorkloadProfile):
                return wl
        return None

    def find_stress_profile(self, name: str) -> Optional[StressWorkloadProfile]:
        for wl in self.workload_profiles:
            if wl.name == name and isinstance(wl, StressWorkloadProfile):
                return wl
        return None

    @classmethod
    def from_yaml(cls, *path: str) -> "ExperimentConfig":
        data = {}
        for p in path:
            with open(p, 'r') as f:
                pdata = yaml.safe_load(f)
                data = {**data, **pdata["experiment"]}

        data["types"] = cls._parse_types(data.get("types"))
        data["workload_profiles"] = cls._parse_workload_profiles(data.get("workload_profiles"))
        data["workload_set"] = cls._parse_workload_set(data.get("workload_set"))
        return cls(**data)

    @staticmethod
    def to_dict(instance) -> dict:
        data = {}
        for field in instance.__dataclass_fields__:
            value = getattr(instance, field)
            if isinstance(value, list):
                data[field] = [
                    ExperimentConfig.to_dict(item) if hasattr(item, "__dataclass_fields__") else item
                    for item in value
                ]
            elif hasattr(value, "__dataclass_fields__"):
                data[field] = ExperimentConfig.to_dict(value)
            else:
                data[field] = value
        return data
