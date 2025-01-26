############################################################
# tee_stream.py
# Write to console in real-time + capture buffer
############################################################

import sys
import io

class TeeStream:
    def __init__(self, real_stream):
        self.real_stream = real_stream
        self.buffer = io.StringIO()

    def write(self, data):
        # write to console immediately
        self.real_stream.write(data)
        self.real_stream.flush()
        # also store in buffer
        self.buffer.write(data)

    def flush(self):
        self.real_stream.flush()

    def getvalue(self):
        return self.buffer.getvalue()
