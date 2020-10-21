from ipykernel.kernelapp import IPKernelApp
from . import HyperKernel

IPKernelApp.launch_instance(kernel_class=HyperKernel)
