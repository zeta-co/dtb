from dtb.rollback.rollback_criteria import RollbackCriteria


class RollbackRequest:
    def __init__(
        self, criteria: RollbackCriteria, user: str, dry_run: bool = False, **kwargs
    ):
        if not isinstance(criteria, RollbackCriteria):
            raise TypeError("criteria must be an instance of RollbackCriteria")
        if not isinstance(user, str):
            raise TypeError("user must be a string")
        if not isinstance(dry_run, bool):
            raise TypeError("dry_run must be a boolean")
        
        self.criteria = criteria
        self.user = user
        self.dry_run = dry_run
        self.kwargs = kwargs
