class DatabricksContext:
    def __init__(self, dbutils) -> None:
        self.context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

    def get_current_user(self) -> str:
        try:
            return self.context.userName().get()
        except Exception as e:
            print(f"Error retrieving current user: {str(e)}")
            return "unknown_user"
