class WorkerError(Exception):
    pass


class JobExecutionError(WorkerError):
    pass


class ReportingError(WorkerError):
    pass
