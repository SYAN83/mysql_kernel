from ipykernel.kernelbase import Kernel
from .utils import MySQLReader
import os
import yaml


class MySQLKernel(Kernel):
    implementation = 'MySQL'
    implementation_version = '0.1'
    language = 'MySQL'
    language_version = '0.1'
    language_info = {
        'name': 'Any text',
        'mimetype': 'text/plain',
        'file_extension': '.sql',
    }
    banner = "MySQL kernel"

    mysql_setting_file = os.path.join(os.path.expanduser('~'), '.local/config/mysql_config.yml')
    _CONFIG = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if os.path.exists(self.mysql_setting_file):
            with open(self.mysql_setting_file, 'r') as f:
                self._CONFIG.update(yaml.load(f))
        self.reader = MySQLReader(**self._CONFIG)

    def do_execute(self, code, silent,
                   store_history=True,
                   user_expressions=None,
                   allow_stdin=False):
        if not silent:
            for msg in self.reader.run(code=code):
                stream_content = {'name': 'stdout', 'text': msg}
                self.send_response(self.iopub_socket, 'stream', stream_content)

        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
               }


if __name__ == '__main__':
    from ipykernel.kernelapp import IPKernelApp
    IPKernelApp.launch_instance(kernel_class=MySQLKernel)