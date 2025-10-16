# CMU DB Group :: Conditional Data Processing (CDP)

I recently joined one of CMU Database Group’s projects, led by Prof. Jignesh Patel. The project aims to use FPGAs to accelerate Groupby aggs—and ultimately other streaming groupby queries—using something called Conditional Data Processing (CDP). In a nutshell, CDP is using different processing methods based on the data. 

More specifically, the project aims to define the three worlds: three methods for groupby agg based on the cardinality of the dataset. Switching from one to another to achieve a best-of-any-world solution is our approach to overcoming the limitations of brittle FPGA accelerators. 

- Small: CAM-based small, brittle, low-latency solution
- Medium: hash tables per thread, merge intermediate aggregates at the end
- Large: a single global hash table using locks, thus contention

My primary job is to build baseline codes for the three worlds in Rust and run distribution streams of increasing cardinalities. I’ll try to keep this document up-to-date.
