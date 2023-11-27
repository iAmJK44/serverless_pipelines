############
# 1. SETUP #
############
import matplotlib.pyplot as plt

# We need this to overcome Python notebooks limitations of too many open files
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
print('Before:', soft, hard)

# Raising the soft limit. Hard limits can be raised only by sudo users
resource.setrlimit(resource.RLIMIT_NOFILE, (10000, hard))
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
print('After:', soft, hard)

import logging
logging.basicConfig(level=logging.INFO)

try:
    from annotation_pipeline import Pipeline
    import lithops
    print('Lithops version: ' + lithops.__version__)
except ImportError:
    print('Failed to import Lithops-METASPACE. Please run "pip install -e ." in this directory.')

# Display Lithops version
import lithops
lithops.__version__

import json

# Input dataset and database (increase/decrease config number to increase/decrease job size)
input_ds = json.load(open('metabolomics/ds_config2.json'))
input_db = json.load(open('metabolomics/db_config2.json'))

print(input_ds)
print(input_db)

##############################
# 2. RUN ANNOTATION PIPELINE #
##############################
from annotation_pipeline.pipeline import Pipeline
import time
time_start = time.time()
pipeline = Pipeline(
    # Input dataset & metabolite database
    input_ds, input_db,
    # Whether to use the pipeline False to accelerate repeated runs with the same database or dataset
    use_ds_cache=True, use_db_cache=True,
    # Set to 'auto' to used the hybrid Serverless+VM implementation when available,
    # True to force Hybrid mode, or False to force pure Serverless mode.
    hybrid_impl='auto'
)

### DATABASE PREPROCESSING ###
# Upload required molecular databases from local machine
pipeline.upload_molecular_databases()
# Parse the database
pipeline.build_database()
# Calculate theoretical centroids for each formula
pipeline.calculate_centroids()

### DATASET PREPROCESSING ###
# Upload the dataset if needed
pipeline.upload_dataset()
# Load the dataset's parser
pipeline.load_ds()
# Parse dataset chunks into IBM COS
pipeline.split_ds()
# Sort dataset chunks to ordered dataset segments
pipeline.segment_ds()
# Sort database chunks to ordered database segments
pipeline.segment_centroids()

### ENGINE ###
# Annotate the molecular database over the dataset by creating images into IBM COS
pipeline.annotate()
# Discover expected false annotations by FDR (False-Discovery-Rate)
pipeline.run_fdr()
time_end = time.time()

### LITHOPS SUMMARY ###
# Display statistics file
from annotation_pipeline.utils import PipelineStats
import pickle
exec_time = time_end-time_start
print(f'Total execution time: {exec_time} s')
PipelineStats.get()

df = PipelineStats.get()
total_exec_time = {'total_execution_time': exec_time}
print(df)

# The pickle contains 2 objects: the dataframe with all the data collected from the functions and another with the total execution time
fname = "config3.pkl"
pickle.dump((df, total_exec_time), open(fname, "wb"))

#########################
# 3. DISPLAY ANNTATIONS #
##############3##########
# Display most annotated molecules statistics
results_df = pipeline.get_results()
top_mols = (results_df
               .sort_values('msm', ascending=False)
               .drop('database_path', axis=1)
               .drop_duplicates(['mol','modifier','adduct']))
print(top_mols.head())
# Download annotated molecules images
formula_images = pipeline.get_images(as_png=False)
# Display most annotated molecules images
import matplotlib.pyplot as plt
for i, (formula_i, row) in enumerate(top_mols.head().iterrows()):
    plt.figure(i)
    plt.title(f'{row.mol}{row.modifier}{row.adduct} - MSM {row.msm:.3f} FDR {row.fdr*100:.0f}%')
    plt.imshow(formula_images[formula_i][0].toarray())
    plt.show()

#######################
# 4. CLEANUP TMP DATA #
#######################
pipeline.clean()



