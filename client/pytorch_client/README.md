# PyTorch Client
### This section serves the purpose of using InfiniCache as a data store during ML training

## Training Steps
* Code for training is listed in the `pytorch_training.py` file.
* For the EFS and EBS datasets, use the `DatasetDisk`
    * Make sure the directory path is the path to the data in your EBS/EFS storage
    * The CIFAR-10 S3 directory is [here](https://cifar10-infinicache.s3.amazonaws.com/).
* For the S3 and InfiniCache datasets, use `DatasetS3` and the name of the appropriate bucket
    * For CIFAR-10 this is "cifar10-infinicache"
* For the EFS and EBS dataloaders, use `DiskLoader` and for for S3 and InfiniCache use `S3Loader` and `InfiniCache` loader, respectively
    * The `dataset_name` parameter is what is used as the prefix for the key in InfiniCache
    * CIFAR-10 img_dims should be (3, 32, 32)
    * Batch size is configurable
* Once the dataset and dataloader have been set up, training can begin
    * Run the `initialize_model` with the model name and the number of channels (CIFAR-10 has 3) for the dataset
    * Resnet50, EfficientNetB4, and DenseNet161 are the pretrained model options
