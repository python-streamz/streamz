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
 - name: Julien Lhermitte
   orcid: 0000-0003-0660-975X
   affiliation: 5
 - name: Daniel B. Allan
   orcid: ?
   affiliation: 4
 - name: Simon J. L. Billinge
   orcid: ?
   affiliation: 2, 3
affiliations:
 - name: Continuum Analytics
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

- This is a small library to manage continuous streams of data, particularly when complex branching and control flow situations arise. 
- This provides a framework for streaming data analysis in pure python with hooks into the Dask task scheduler for automatic parallelization.
- Streamz is similar to reactive
programming systems like [RxPY](https://github.com/ReactiveX/RxPY) or big
data streaming systems like [Flink](https://flink.apache.org), [Beam](https://beam.apache.org/get-started/quickstart-py) or [Spark Streaming](https://beam.apache.org/get-started/quickstart-py).
- This software forms the backbone for data processing at the NSLS-II x-ray powder diffraction and complex materials scattering data analysis pipelines.

Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

# References

# Acknowledgments
The development of this software in the Billinge group was
supported by the U.S. National Science Foundation through
grant DMREF-1534910