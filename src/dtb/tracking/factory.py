from .tracker import Tracker
from .delta_version_tracker import DeltaVersionTracker


class TrackerFactory:
    @staticmethod
    def create_tracker(type: str, *args, **kwargs) -> Tracker:
        trackers = {"delta_version": DeltaVersionTracker}
        return trackers[type](*args, **kwargs)
