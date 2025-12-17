Use cases
=========

ETL applications
----------------

  In LorryStream_, we use Streamz at the core for relaying data from streaming
  sources into CrateDB_, because we have been looking for something smaller
  and more concise than Beam, Flink, or Spark. Streamz gives us the freedom
  to use advanced stream processing and flow control primitives within Python
  applications to feed databases, without the need to spin up compute clusters,
  or deal with Java class paths.

In this spirit, LorryStream is effectively just a humble CLI interface for
Streamz.

Telemetry readings
------------------

  At my company, we use it at two different points in our data processing to
  create a disjoint subcover of a noisy stream of telemetry readings and then
  blend in metadata from users and external authoritative resources to give
  users an understanding of what (and what kinds of) events are happening in
  their space.


.. _CrateDB: https://github.com/crate/crate
.. _LorryStream: https://lorrystream.readthedocs.io/
