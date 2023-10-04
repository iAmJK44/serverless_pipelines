# Cloudbutton geospatial use case: Model calculation
In this pipeline we perform serverless Model calculation from LiDAR files. It is computed using the serverless architecture [Lithops](https://github.com/lithops-cloud/lithops).

## Prerequisites
To execute this notebook you need:
   - An AWS Account.
   - Setup Lithops to work with AWS Lambda.

## Instructions

1. Build the runtime with `ibm_cf_3-9.Dockerfile` in the [`runtime`](https://github.com/cloudbutton/geospatial-usecase/tree/main/calculate-models/runtime) directory :

   ```bash
   $ lithops runtime build -f ibm_cf_3-9.Dockerfile model-calculation:1
   ```
2. Configure Lithops to use the built runtime (e.g. `model-calculation:1`). 

3. Download the [LiDAR files](https://www.icgc.cat/es/Descargas/Elevaciones/Datos-lidar)  and upload them to an S3 bucket named *`cb-geospatial-wildfire`* .

4. Follow the instructions in the notebook to execute the code.

**NOTE:**  you can change the names of the S3 bucket but then you have to change them in the notebook too.
