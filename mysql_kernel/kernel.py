from ipykernel.kernelbase import Kernel

class MySQLKernel(Kernel):
    implementation = 'MySQL'
    implementation_version = '0.1'
    language = 'no-op'
    language_version = '0.1'
    language_info = {
        'name': 'Any text',
        'mimetype': 'text/plain',
        'file_extension': '.sql',
    }
    banner = "MySQL kernel"

    def do_execute(self, code, silent, store_history=True, user_expressions=None,
                   allow_stdin=False):
        if not silent:
            stream_content = {'name': 'stdout', 'text': 'MySQL: {}\n'.format(code)}
            self.send_response(self.iopub_socket, 'stream', stream_content)
            self.send_response(self.iopub_socket, 'stream', stream_content)

        return {'status': 'ok',
                # The base class increments the execution count
                'execution_count': self.execution_count,
                'payload': [],
                'user_expressions': {},
               }
