Preprocessing datasets for OmniSketch experiments.

In this repository, we can preprocess streams to have a desired % deletes.

- CAIDA & TPC-DS customer table:
  We need to (1) split dataset in residu and noise and (2) mix residu, noise inserts (sign 1) and noise deletes (sign -1).

- Synthetic data:
 Before splitting and mixing, we need to generate the file using the CreateDataset class.


Change the HPC_{datasetname}.json files to adjust settings like #attributes, Zipfian distribution, size, % of deletes.



  
