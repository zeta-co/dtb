from dataclasses import dataclass


@dataclass
class ValidationThreshold:
    type: str  # 'percentage' or 'absolute'
    value: float
