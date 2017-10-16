---
title: 'Streamz: A small library to manage continuous streams of data'
tags:
  - Stream
  - Dask
authors:
 - name: Matthew Rocklin
   orcid: ?
   affiliation: 1
 - name: Christopher J. Wright
   orcid: 0000-0003-2522-7028
   affiliation: 2
 - name: Julien R. Lhermitte
   orcid: 0000-0003-0660-975X
   affiliation: 5
 - name: Daniel B. Allan
   orcid: 0000-0002-5947-6017
   affiliation: 4
 - name: Stuart I. Campbell
   orcid: 0000-0001-7079-0878
   affiliation: 4
 - name: Kevin G. Yager
   orcid: 0000-0001-7745-2513
   affiliation: 5
 - name: Simon J. L. Billinge
   orcid: 0000-0002-9734-4998
   affiliation: 2, 3
affiliations:
 - name: Anaconda Inc.
   index: 1
 - name: Department of Applied Physics and Applied Mathematics, Columbia University
   index: 2
 - name: Condensed Matter Physics and Materials Science Department, Brookhaven National Laboratory
   index: 3
 - name: NSLS-II, Brookhaven National Laboratory
   index: 4
 - name: Center for Functional Nanomaterials, Brookhaven National Laboratory
   index: 5
date: 19 September 2017
bibliography: paper.bib
---

# Summary

Streamz is a small library to manage continuous streams of data, 
particularly when complex branching and control flow situations arise.
The library is similar to reactive programming systems like RxPY [@RxPy] or big
data streaming systems like Apache Flink [@Flink], Apache Beam [@Beam] or 
Apache Spark Streaming [@Spark].
This software forms the backbone for data processing at the 
NSLS-II X-ray powder diffraction and complex materials scattering data 
analysis pipelines.

Streamz is designed specifically to be:
1. Lightweight: You can import it and go without setting up any infrastructure. It can run (in a limited way) on a Dask cluster or on an event loop, but it’s also fully operational in your local Python thread. There is no magic in the common case. Everything up until time-handling runs with tools that you learn in an introductory programming class.
1. Small and maintainable: The codebase is currently a few hundred lines. It is also, I claim, easy for other people to understand. Here is the code for filter:
1. Composable with Dask: Handling distributed computing is tricky to do well. Fortunately this project can offload much of that worry to Dask. The dividing line between the two systems is pretty clear and, I think, could lead to a decently powerful and maintainable system if we spend time here.
1. Low performance overhead: Because this project is so simple it has overheads in the few-microseconds range when in a single process.
1. Pythonic: All other streaming systems were originally designed for Java/Scala engineers. While they have APIs that are clearly well thought through they are sometimes not ideal for Python users or common Python applications.

## Architecture
Pipelines are created by combine various operation nodes which listen to each 
other, perform some operation on the data, and emit the mutated data downstream.
The basic structure of each node has three components: an init which describes
the which upstream nodes the current node listens to, an update which describes
what the node does when it receives the data (applies a function, 
filters the data, combines data, etc.), and an emit which describes how the data
is passed to downstream nodes which are listening. 

## Usage
This library is used at two X-ray beamlines at the NSLS-II. SHED and xpdAn at
the x-ray powder diffraction beamline and SciStreams at CMS use streamz to 
process incoming data live. In these cases streamz is combined with pieces
of the scientific stack (numpy, scipy, and matplotlib among others) to process
2D images to interpretable data.

# References

# Acknowledgments
Software and writing contributions from Wright and Billinge were supported by the U.S. National Science Foundation through
grant DMREF-1534910
This research used resources of the Center for Functional Nanomaterials and the NSLS-II, which is a U.S. DOE Office of Science Facility, at Brookhaven National Laboratory under Contract No. DE-SC0012704