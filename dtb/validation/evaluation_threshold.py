from dataclasses import dataclass


@dataclass
class EvaluationThreshold:
    type: str  # 'percentage' or 'absolute'
    value: float
