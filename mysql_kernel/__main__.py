from ipykernel.kernelapp import IPKernelApp
from . import MySQLKernel

IPKernelApp.launch_instance(kernel_class=MySQLKernel)
