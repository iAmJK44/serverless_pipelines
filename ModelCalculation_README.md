# Cloudbutton geospatial use case: Model calculation
In this pipeline we perform serverless Model calculation from LiDAR files. It is computed using the serverless architecture [Lithops](https://github.com/lithops-cloud/lithops).

## Prerequisites
To execute this notebook you need:
   - An AWS Account.
   - Setup Lithops to work with AWS Lambda.
   - Upgrade the packages in [requirements.txt](https://github.com/cloudbutton/geospatial-usecase/blob/main/requirements.txt) and then install the following packages with the following versions:
      * Lithops 2.9.0 and above
      * botocore 1.31.62

## Instructions

1. Build the runtime with `Dockerfile` in the [`runtime`](https://github.com/cloudbutton/geospatial-usecase/tree/main/calculate-models/runtime) directory :

   ```bash
   $ lithops runtime build -f Dockerfile model-calculation:1
   ```
2. Configure Lithops to use the built runtime (e.g. `model-calculation:1`). 

3. Create an S3 bucket named *`cb-geospatial-wildfire`*. 

4. Download the [LiDAR files](https://www.icgc.cat/es/Descargas/Elevaciones/Datos-lidar)  and store them in a directory called *`input-las-tiles`* in the execution directory .

5. Follow the instructions in the notebook to execute the code.

**NOTE:**  you can change the names of the S3 bucket but then you have to change them in the notebook too.
