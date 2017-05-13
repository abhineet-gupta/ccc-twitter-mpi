# Twitter Analytics via MPI

## COMP90024 - University of Melbourne

### Cluster and Cloud Computing - Assignment 1 (Individual)
---
A parallelized application leveraging the University of Melbourne HPC facility SPARTAN. It  searches a large geocoded Twitter dataset to identify tweet hotspots around Melbourne.

Read full assignment details [here](data/a1-spec.pdf)

---
Run non-MPI version of the program via ```./src/<script>.py <twitter data file> <grid file>```.
E.g.
```bash
./src/CCC_A1_nonMPI.py ../data/tinyTwitter.json ../data/melbGrid.json
```
_For MPI: replace with correct script within [src](src)._
