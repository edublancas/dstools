# `dstools.pipeline` philosophy

* No redundant computations - 
* Avoid redundancy (by passing downstream params)
* Explicit (we could build the DAG based on declared requirements but that hides a ground truth about the pipeline)
* No shared state (DAG is just a collection of tasks - as opposed to a class)
* Designed for heterogeneous computation, the library does not make any assumption about where the code should run (locally or on a different machine) or what (language agnostic)
* Transparent - A DAG can be easily plotted or a table can be generated to inspect its status
* No hardcoded paths/locations - each product (file/db table/etc)
