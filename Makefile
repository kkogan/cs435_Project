# These might differ per person, so create a config.env file (it will be ignored by git)
-include config.env
# # config.env
# MASTER_NODE=dover
# MASTER_PORT=30476
# PROJ_ROOT=~/cs435/cs435_Project
# HDFS_ROOT=/cs435/cs435_Project

DATA_PREP_PACKAGE := data_prep
PYTHON_VERSION := 3.9.16
PYTHON_VENV_DIR := python3.9
HDFS_OUTPUT_ROOT := $(HDFS_ROOT)/output
LOCAL_OUTPUT_ROOT := $(PROJ_ROOT)/output
SHARED_INPUT_ROOT :=  ~cs435/cs435a



####################################################################################
# build/run

CLUSTER_ARGS = --master spark://$(MASTER_NODE):$(MASTER_PORT)

SSH_NODE = $(MASTER_NODE)
RUN_OUT_DIR =
SPARK_ARGS =
RUN_SCRIPT = 
SCRIPT_ARGS =
define RUN_SPARK
	ssh $(SSH_NODE) "\
		cd $(PROJ_ROOT) && \
		spark-submit $(SPARK_ARGS) $(RUN_SCRIPT) $(SCRIPT_ARGS)\
	";
endef


dependencies:
	pip install -U -r requirements.txt


####################################################################################
# Data Processing


DATA_DIR_V0 := CS435_Plant_Data
DATA_DIR_V1 := CS435_Plant_Data_v1
DATA_DIR_V2 := CS435_Plant_Data_v2


prepare-dataset-from-scratch: dataset-v2 manifest-v2 augment-v2


dataset-v2: SOURCE_DIR = $(HDFS_ROOT)/$(DATA_DIR_V0)/
dataset-v2: DEST_DIR = $(HDFS_ROOT)/$(DATA_DIR_V2)/
dataset-v2:
	hadoop fs -rm -r -f $(DEST_DIR)
	hadoop fs -ls -R $(SOURCE_DIR) | \
		grep '^d' | awk '{print $$8}' | \
		grep -v 'Blueberry\|Orange\|Raspberry\|Soybean\|Squash' | \
		while read dir; do \
			relative_path=$${dir#$(SOURCE_DIR)}; \
			if [[ $$relative_path != "" && $$relative_path != "train" && $$relative_path != "valid" && $$relative_path != "test" ]]; then \
				classname=$$(basename "$$dir") && \
				echo "hadoop fs -mkdir -p $(DEST_DIR)$${classname}" && \
				hadoop fs -mkdir -p "$(DEST_DIR)$${classname}" && \
				echo "hadoop fs -cp" "$${dir}/*" "$(DEST_DIR)$${classname}/" && \
				hadoop fs -cp "$${dir}/*" "$(DEST_DIR)$${classname}/" ; \
			fi \
		done


manifest-v2: RUN_SCRIPT = $(DATA_PREP_PACKAGE)/manifest.py
manifest-v2: SCRIPT_ARGS = --hdfs_path=hdfs://$(HDFS_ROOT)/$(DATA_DIR_V2) --output_path=$(LOCAL_OUTPUT_ROOT)/manifest_v2.csv
manifest-v2:
	python $(RUN_SCRIPT) $(SCRIPT_ARGS)


TIME := $(shell date +%Y-%m-%d-%H%M%S)
augment-v2: LIMIT_IMGS = 0
augment-v2: INPUT_DIR = $(HDFS_ROOT)/$(DATA_DIR_V2)
augment-v2: RUN_SCOPED_OUTPUT_DIR = $(DATA_DIR_V2)_$@/$(TIME)
augment-v2: OUTPUT_DIR = output/$(RUN_SCOPED_OUTPUT_DIR)
augment-v2: OLD_MANIFEST_LOCAL = $(LOCAL_OUTPUT_ROOT)/manifest_v2.csv
augment-v2: MANIFEST_INPUT_HDFS = hdfs://$(INPUT_DIR)/manifest_v2.csv
augment-v2: MANIFEST_OUTPUT_HDFS = hdfs://$(HDFS_ROOT)/$@/$(TIME)/new_manifest
augment-v2: GET_OUTPUTS_HDFS_SOURCE = $(MANIFEST_OUTPUT_HDFS)
augment-v2: GET_OUTPUTS_LOCAL_DEST = $(LOCAL_OUTPUT_ROOT)/$(RUN_SCOPED_OUTPUT_DIR)
augment-v2: COPY_TO_SHARED_AUGMENTS_OUTPUT_DIR = $(OUTPUT_DIR)
augment-v2: COPY_TO_SHARED_AUGMENTS_INPUT_DIR = $(INPUT_DIR)
augment-v2: COPY_TO_SHARED_DEST_DIR = $(SHARED_INPUT_ROOT)/$(RUN_SCOPED_OUTPUT_DIR)
augment-v2: RUN_SCRIPT = $(DATA_PREP_PACKAGE)/augment.py
augment-v2: SPARK_ARGS = $(CLUSTER_ARGS) --driver-memory 4G --py-files $(DATA_PREP_PACKAGE)/augmentations_config.py
augment-v2: SCRIPT_ARGS = --local=0 --hdfs_input_root=hdfs://$(INPUT_DIR)/ --local_output_root=$(OUTPUT_DIR) --manifest_input=$(MANIFEST_INPUT_HDFS) --manifest_output=$(MANIFEST_OUTPUT_HDFS) --limit=$(LIMIT_IMGS)
augment-v2: $(LOCAL_OUTPUT_ROOT)
	hadoop fs -put -f $(OLD_MANIFEST_LOCAL) $(MANIFEST_INPUT_HDFS)
	$(RUN_SPARK)
	hadoop fs -get -f $(MANIFEST_OUTPUT_HDFS)/*.csv $(GET_OUTPUTS_LOCAL_DEST)/
	mv $$(find $(GET_OUTPUTS_LOCAL_DEST) -maxdepth 1 -name "*.csv" | head -n 1) $(GET_OUTPUTS_LOCAL_DEST)/augmented_manifest.csv
	cp $(OLD_MANIFEST_LOCAL) $(GET_OUTPUTS_LOCAL_DEST)/old_manifest.csv
	python $(DATA_PREP_PACKAGE)/merge_manifests.py $(GET_OUTPUTS_LOCAL_DEST)/old_manifest.csv $(GET_OUTPUTS_LOCAL_DEST)/augmented_manifest.csv $(GET_OUTPUTS_LOCAL_DEST)/new_manifest.csv 
	$(COPY_TO_SHARED)


COPY_TO_SHARED_AUGMENTS_OUTPUT_DIR =
COPY_TO_SHARED_AUGMENTS_INPUT_DIR =
COPY_TO_SHARED_DEST_DIR =
define COPY_TO_SHARED
	mkdir -p $(COPY_TO_SHARED_DEST_DIR) && \
	hadoop fs -get -f $(COPY_TO_SHARED_AUGMENTS_INPUT_DIR)/* $(COPY_TO_SHARED_DEST_DIR)/ && \
		cp -R $(COPY_TO_SHARED_AUGMENTS_OUTPUT_DIR)/* $(COPY_TO_SHARED_DEST_DIR)/ && \
		chmod -R 777 $(COPY_TO_SHARED_DEST_DIR)
endef

# shouldn't need to use this going forward, since it it part of augment-v2 above
copy-to-shared-v2: COPY_TO_SHARED_AUGMENTS_OUTPUT_DIR = output/CS435_Plant_Data_v2/augment-v2/2023-11-15-231056
copy-to-shared-v2: COPY_TO_SHARED_AUGMENTS_INPUT_DIR = $(HDFS_ROOT)/$(DATA_DIR_V2)
copy-to-shared-v2: COPY_TO_SHARED_DEST_DIR = $(SHARED_INPUT_ROOT)/CS435_Plant_Data_v2_augment-v2/2023-11-15-231056
copy-to-shared-v2:
	$(COPY_TO_SHARED)

print-dataset-v2: PRINT_DATASET_DIR = $(HDFS_ROOT)/$(DATA_DIR_V2)
print-dataset-v2:
	$(PRINT_DATASET)


load-hdfs-input:
	hadoop fs -ls /
	hadoop fs -mkdir -p $(HDFS_ROOT)/$(DATA_DIR_V0)
	hadoop fs -put -f $(SHARED_INPUT_ROOT)/$(DATA_DIR_V0) $(HDFS_ROOT)/
	hadoop fs -ls $(HDFS_ROOT)/$(DATA_DIR_V0)


delete-hdfs-input:
	hadoop fs -rm -r $(HDFS_ROOT)/$(DATA_DIR_V0)


hdfs-status:
	hdfs dfsadmin -report


$(LOCAL_OUTPUT_ROOT):
	mkdir -p $@


PRINT_DATASET_DIR = 
define PRINT_DATASET
	hadoop fs -ls -R $(PRINT_DATASET_DIR)/ | \
		grep '^-' | awk '{print $$8}' | xargs -n 1 dirname | \
		awk -F/ '{subdir=$$(NF); parent=$$(NF-1); split(subdir, a, "___"); healthy=(a[2]=="healthy"?1:0); print subdir, parent, a[1], healthy}' | \
		sort | uniq -c
endef


SOURCE_DIR =
DEST_DIR =  
define FILTER_DATASET_1
	hadoop fs -ls -R $(SOURCE_DIR)/ | \
		grep '^d' | awk '{print $$8}' | \
		grep -v 'Blueberry\|Orange\|Raspberry\|Soybean\|Squash' | \
		while read dir; do \
			relative_path=$${dir#$(SOURCE_DIR)}; \
			if [[ $$relative_path != "" && $$relative_path != "train" && $$relative_path != "valid" && $$relative_path != "test" ]]; then \
				echo "hadoop fs -mkdir -p $(DEST_DIR)$${relative_path}" && \
				hadoop fs -mkdir -p "$(DEST_DIR)$${relative_path}" && \
				echo "hadoop fs -cp" "$${dir}/*" "$(DEST_DIR)$${relative_path}/" && \
				hadoop fs -cp "$${dir}/*" "$(DEST_DIR)$${relative_path}/" ; \
			fi; \
		done
endef

GET_OUTPUTS_HDFS_SOURCE=
GET_OUTPUTS_LOCAL_DEST=
define GET_OUTPUTS
	rm -rf $(GET_OUTPUTS_LOCAL_DEST) && \
	mkdir -p $(GET_OUTPUTS_LOCAL_DEST) && \
	hadoop fs -get -f $(GET_OUTPUTS_HDFS_SOURCE)/* $(GET_OUTPUTS_LOCAL_DEST)/ && \
	find $(GET_OUTPUTS_LOCAL_DEST)/  -type f -exec sh -c 'echo "{}"; head -n 5 "{}"; echo' \;
endef



####################################################################################
# Obsolete


dataset-v1: SOURCE_DIR = $(HDFS_ROOT)/$(DATA_DIR_V0)/
dataset-v1: DEST_DIR = $(HDFS_ROOT)/$(DATA_DIR_V1)/
dataset-v1:
	hadoop fs -rm -r -f $(DEST_DIR)
	hadoop fs -ls -R $(SOURCE_DIR) | \
		grep '^d' | awk '{print $$8}' | \
		grep -v 'Blueberry\|Orange\|Raspberry\|Soybean\|Squash' | \
		while read dir; do \
			relative_path=$${dir#$(SOURCE_DIR)}; \
			if [[ $$relative_path != "" && $$relative_path != "train" && $$relative_path != "valid" && $$relative_path != "test" ]]; then \
				echo "hadoop fs -mkdir -p $(DEST_DIR)$${relative_path}" && \
				hadoop fs -mkdir -p "$(DEST_DIR)$${relative_path}" && \
				echo "hadoop fs -cp" "$${dir}/*" "$(DEST_DIR)$${relative_path}/" && \
				hadoop fs -cp "$${dir}/*" "$(DEST_DIR)$${relative_path}/" ; \
			fi; \
		done

print-dataset-v1: PRINT_DATASET_DIR = $(HDFS_ROOT)/$(DATA_DIR_V1)/
print-dataset-v1:
	$(PRINT_DATASET)


dataset-v0: delete-hdfs-input load-hdfs-input

print-dataset-v0: PRINT_DATASET_DIR = $(HDFS_ROOT)/$(DATA_DIR_V0)/
print-dataset-v0:
	$(PRINT_DATASET)