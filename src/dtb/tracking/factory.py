from .tracker import Tracker
from .tracker_delta_version import DeltaVersionTracker


class TrackerFactory:
    @staticmethod
    def create_tracker(type: str, *args, **kwargs) -> Tracker:
        trackers = {"delta_version": DeltaVersionTracker}
        return trackers[type](*args, **kwargs)
