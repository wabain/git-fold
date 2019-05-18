class Fatal (Exception):
    def __init__(self, message, returncode=1, extended=None):
        super().__init__(message)
        self.returncode = returncode
        self.extended = extended
