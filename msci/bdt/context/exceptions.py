class BDTError(Exception):
    def __init__(self, msg, job_id=None, status=None):
        self.job_id = job_id
        self.status = status
        Exception.__init__(self, msg)

