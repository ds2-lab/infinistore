# The path to the InfiniBench source code
BENCH_HOME=../../infinibench
# The path to the experiment workloads
WORKLOAD_HOME=../../infinistore-reproducibility/workloads
# AWS region
REGION=us-east-1
# S3 bucket for storing the experiment data.
S3_BUCKET_DATA=jzhang33.default
# Server address formatter for ElastiCache cluster. Replace the variable segment with %04d.
# In current settings, the cluster address is not supported.
EC_ADDR=redis-%04d-001.fxxiur.0001.use1.cache.amazonaws.com:6379
# S3 bucket for storing the durable data if ElastiCache runs in the cache mode.
S3_BUCKET_EC_BACKUP=tianium.ec.backup