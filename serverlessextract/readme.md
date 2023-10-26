# Astronomica-interferometry
In this pipeline we perform radio interferomic data processing carrying out all the phases: rebinning, calibrationa and imaging. It is computed using the serverless architecture [Lithops](https://github.com/lithops-cloud/lithops).

<img src="https://github.com/iAmJK44/serverless_benchmarks/assets/97289591/321e8834-e178-462e-a6a8-956de05c8d3a"  width="900" height="450">

## Prerequisites
To execute this notebook you need:
   - An AWS Account.
   - Setup Lithops to work with AWS Lambda.

## Setup

1. Download the [data](https://share.obspm.fr/s/ezBfciEfmSs7Tqd?path=%2FDATA)  and extract it in a directory similar to *`/home/user/Downloads/entire_ms/SB205.MS/`* . Change the user name in the path of this line of the main of [partition.py](partition/partition.py) file:

   ```bash
   $ p = Partitioner("/home/ayman/Downloads/entire_ms/SB205.MS/")
   ```
   In case you downloaded another set of data instead of SB205.MS, change the name too.

2. Setup Lithops for AWS backend.

3. Build the runtime in the [`docker`](docker/) directory :

   ```bash
   $ lithops runtime build -f Dockerfile serverless-extract:1
   ```
4. Configure Lithops to use the built runtime (e.g. `serverless-extract:1`). 

5. Create an S3 bucket named *`aymanb-serverless-genomics`* to upload the data.

6. Run `partition.py` located in [partition](partition/) directory. This will create and upload the .ms files to the S3 bucket divided in 70 partition by default.

   ```bash
   $ cd ./partition/
   $ python3 partition.py
   ```

7. Run the `pipeline.py` file. This file performs all the phases of the pipeline [rebinning, calibration, imaging]:
   ```bash
   $ python3 pipeline.py
   ```
   More information on how it works in this [link](https://share.obspm.fr/s/ezBfciEfmSs7Tqd?dir=undefined&path=%2F&openfile=19186544).

8. The results obtained should look similar to the images in [/stats/stats/](stats/stats/) .

**NOTE:**  you can change the names of the S3 bucket and the number of partitions editing the `pipeline.py`and `partition.py`files.
