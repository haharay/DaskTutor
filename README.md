# Dask Tutorial

本教程最后一次是在SciPy 2020上讲的，这是一次虚拟会议。
[A video of the SciPy 2020 tutorial is available online](https://www.youtube.com/watch?v=EybGGLbLipI).

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/dask/dask-tutorial/master?urlpath=lab)
[![Build Status](https://github.com/dask/dask-tutorial/workflows/CI/badge.svg)](https://github.com/dask/dask-tutorial/actions?query=workflow%3ACI)

Dask在大于内存的数据集上提供多核执行。

我们可以从高、低两个层次的角度来看dask:

* **高级集合：** Dask提供高级Array、Bag、DataFrame、集合，模仿NumPy、list和Pandas，但可以并行运行在的数据集，不适合放在主内存中。 Dask 的高级集合是   NumPy和Pandas的替代品，用于大型数据集。
* **低级调度器：** Dask提供动态任务调度器，可实现以下功能 并行执行任务图。 这些执行引擎为   上面提到的高级集合，但也可以为自定义提供动力。  用户定义的工作负载。 这些调度器是低延迟（约1ms）和   努力在较小的内存占用范围内运行计算。 Dask的   调度器是直接使用 "线程 "或 "线程 "的替代方案。  复杂情况下的 "多处理 "库或其他任务调度。 系统，如 "Luigi "或 "IPython并行"。

不同的用户在不同的层次上进行操作，但了解以下内容是有用的 都有。 本教程将在高级使用`dask.array`和 `dask.dataframe`(甚至是部分)和低级别的dask图形和使用。调度员（奇数节。

## Prepare

#### 1. You should clone this repository

    git clone http://github.com/dask/dask-tutorial

然后安装必要的软件包。
有三种不同的方法可以实现，选择最适合你的方法，***只选择一个选项***。
它们依次是

#### 2a) Create a conda environment (preferred)

In the main repo directory

    conda env create -f binder/environment.yml
    conda activate dask-tutorial
    jupyter labextension install @jupyter-widgets/jupyterlab-manager
    jupyter labextension install @bokeh/jupyter_bokeh

#### 2b) Install into an existing environment

You will need the following core libraries

    conda install numpy pandas h5py pillow matplotlib scipy toolz pytables snakeviz scikit-image dask distributed -c conda-forge

You may find the following libraries helpful for some exercises

    conda install python-graphviz -c conda-forge

Note that this options will alter your existing environment, potentially changing the versions of packages you already
have installed.

#### 2c) Use Dockerfile

You can build a docker image out of the provided Dockerfile.

    $ docker build . # This will build using the same env as in a)

Run a container, replacing the ID with the output of the previous command

    $ docker run -it -p 8888:8888 -p 8787:8787 <container_id_or_tag>

The above command will give an URL (`Like http://(container_id or 127.0.0.1):8888/?token=<sometoken>`) which
can be used to access the notebook from browser. You may need to replace the given hostname with "localhost" or
"127.0.0.1".

#### You should follow only one of the options above!

### Launch notebook

From the repo directory

    jupyter notebook

Or

    jupyter lab

This was already done for method c) and does not need repeating.

## Links

*  Reference
    *  [Docs](https://dask.org/)
    *  [Examples](https://examples.dask.org/)
    *  [Code](https://github.com/dask/dask/)
    *  [Blog](https://blog.dask.org/)
*  Ask for help
    *   [`dask`](http://stackoverflow.com/questions/tagged/dask) tag on Stack Overflow, for usage questions
    *   [github issues](https://github.com/dask/dask/issues/new) for bug reports and feature requests
    *   [gitter chat](https://gitter.im/dask/dask) for general, non-bug, discussion
    *   Attend a live tutorial

## Outline

0. [Overview](00_overview.ipynb) - dask's place in the universe.

1. [Delayed](01_dask.delayed.ipynb) - the single-function way to parallelize general python code.

1x. [Lazy](01x_lazy.ipynb) - some of the principles behind lazy execution, for the interested.

2. [Bag](02_bag.ipynb) - the first high-level collection: a generalized iterator for use
with a functional programming style and to clean messy data.

3. [Array](03_array.ipynb) - blocked numpy-like functionality with a collection of
numpy arrays spread across your cluster.

7. [Dataframe](04_dataframe.ipynb) - 集群上多个pandas数据框上的并行化操作。

5. [Distributed](05_distributed.ipynb) - Dask's scheduler for clusters, with details of
how to view the UI.

6. [Advanced Distributed](06_distributed_advanced.ipynb) - further details on distributed
computing, including how to debug.

7. [Dataframe Storage](07_dataframe_storage.ipynb) - efficient ways to read and write
dataframes to disc.

8. [Machine Learning](08_machine_learning.ipynb) - applying dask to machine-learning problems.
