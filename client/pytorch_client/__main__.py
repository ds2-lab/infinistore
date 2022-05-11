import argparse
import torch
import os

import pytorch_training
from pytorch_training import initialize_model, run_training_get_results
from infinicache_dataloaders import DatasetS3, S3Loader
from updated_datasets import DatasetDisk, MiniObjDataset
from pathlib import Path
from torch.utils.data import DataLoader
import utils
import logging_utils

class SmartFormatter(argparse.HelpFormatter):
  """Add option to split lines in help messages"""

  def _split_lines(self, text, width):
    # this is the RawTextHelpFormatter._split_lines
    if text.startswith('R|'):
        return text[2:].splitlines()
    return argparse.HelpFormatter._split_lines(self, text, width)

# class RemainderAction(argparse.Action):
#     """Action to create script attribute"""
#     def __call__(self, parser, namespace, values, option_string=None):
#         if not values:
#             raise argparse.ArgumentError(
#                 self, "can't be empty")

#         setattr(namespace, self.dest, values[0])
#         setattr(namespace, "argv", values) 

  
def main():
  parser = argparse.ArgumentParser(description=__doc__, formatter_class=SmartFormatter)
  parser.add_argument("-v", "--version", action="version",
                      version="pytorch_client {}".format("1"))
  parser.add_argument("--dataset", action='store', type=str, help="Dataset to use, choosing from mnist, imagenet or cifar.", default="cifar")
  parser.add_argument("--ready", action='store_true', help="Set if data is loaded to the data source")
  parser.add_argument("--cpu", action='store_true', help="Using cpu for training.")
  parser.add_argument("--disk_source", action='store', type=str, help="Load dataset from disk and specifiy the path.", default="")
  parser.add_argument("--s3_source", action='store', type=str, help="Load dataset from S3 and specifiy the bucket.", default="cifar10-infinicache")
  parser.add_argument("--s3_train", action='store', type=str, help="Load training set from S3 and specifiy the bucket.", default="infinicache-cifar-train")
  parser.add_argument("--s3_test", action='store', type=str, help="Load test set from S3 and specifiy the bucket.", default="infinicache-cifar-test")
  parser.add_argument("--loader", action='store', type=str, help="Dataloader used, choosing from disk, s3, or infinicache.", default="disk")
  parser.add_argument("--model", action='store', type=str, help="Pretrained model, choosing from resnet, efficientnet or densenet.", default="")
  parser.add_argument("--batch", action='store', type=int, help="The batch size.", default=64)
  parser.add_argument("--epochs", action='store', type=int, help="Max epochs.", default=100)
  parser.add_argument("--accuracy", action='store', type=float, help="Target accuracy.", default=1.0)
  parser.add_argument("--benchmark", action='store_true', help="Simply benchmark the pretrained model.")
  parser.add_argument("--workers", action='store', type=int, help="Number of workers.", default=0)
  parser.add_argument("-o", "--output", action='store', type=str, help="Output file", default="")

  args, _ = parser.parse_known_args()

  if args.output != "":
    pytorch_training.DATALOG = logging_utils.get_logger(pytorch_training.DATALOG.name, logging_utils.set_file_handler(args.output))

  normalize_cifar = utils.normalize_image(True)
  trainset, testset = None, None
  trainloader, testloader = None, None

  # Define the dataloader
  if args.dataset == "mnist":
    img_dims = (1, 28, 28)
  elif args.dataset == "cifar":
    img_dims = (3, 32, 32)
  else:
    img_dims = (3, 256, 256)

  # Define the dataset
  loadtestset = testset != ""
  if args.disk_source != "":
    if args.s3_source != "":
      if args.ready:
        args.s3_source = ""
      
      if args.dataset == "cifar":
        args.disk_source = os.path.join(args.disk_source, "full")

      # trigger download
      trainset = DatasetDisk([Path(args.disk_source)], s3_bucket = args.s3_source, label_idx=0, dataset_name=args.dataset, img_transform=normalize_cifar)

      if args.dataset == "cifar":
        train_source, test_source = utils.split_cifar_data(
          args.disk_source,
          os.path.join(os.path.dirname(__file__), "cifar_test_fnames.txt"),
        )
        trainset = DatasetDisk(train_source, label_idx=0, dataset_name=args.dataset, img_transform=normalize_cifar)
        testset = DatasetDisk(test_source, label_idx=0, dataset_name=args.dataset, img_transform=normalize_cifar)

    else:
      if args.ready:
        args.s3_train = ""
        args.s3_test = ""

      trainset = DatasetDisk(
        filepaths = [Path(os.path.join(args.disk_source, "training"))], s3_bucket = args.s3_train, label_idx=0, dataset_name=args.dataset, img_transform=normalize_cifar
      )
      if loadtestset:
        testset = DatasetDisk(
          filepaths = [Path(os.path.join(args.disk_source, "test"))], s3_bucket = args.s3_test, label_idx=0, dataset_name=args.dataset, img_transform=normalize_cifar
        )

  elif args.loader == "ininficache":
    if args.s3_train == "":
      args.s3_train = args.s3_source

    trainset = MiniObjDataset(
        args.s3_train,
        label_idx=0,
        channels=True,
        dataset_name=args.dataset,
        img_dims=img_dims,
        obj_size=16,
        img_transform=normalize_cifar,
    )
    trainset.initial_set_all_data()
    if loadtestset:
      testset = MiniObjDataset(
          args.s3_test,
          label_idx=0,
          channels=True,
          dataset_name=args.dataset,
          img_dims=(3, 32, 32),
          obj_size=16,
          img_transform=normalize_cifar,
      )
      testset.initial_set_all_data()

  else:
    if args.s3_train == "":
      args.s3_train = args.s3_source

    trainset = DatasetS3(
        args.s3_train, label_idx=0, channels=True, testing=args.benchmark
    )
    if loadtestset:
      testset = DatasetS3(
          args.s3_test, label_idx=0, channels=True, testing=args.benchmark
      )

  # Define the dataloader
  if args.loader == "s3":
    trainloader = S3Loader(
      trainset, dataset_name=args.dataset, img_dims=img_dims, batch_size=args.batch
    )
    if loadtestset:
      testloader = S3Loader(
        testset, dataset_name=args.dataset, img_dims=img_dims, batch_size=args.batch
      )
  else:
    collate_fn = None
    if args.loader == "infinicache":
      collate_fn = utils.infinicache_collate
    trainloader = DataLoader(
        trainset, batch_size=args.batch, shuffle=True, num_workers=args.workers, collate_fn=collate_fn, pin_memory=True
    )
    if loadtestset:
      testloader = DataLoader(
          testset, batch_size=args.batch, shuffle=True, num_workers=args.workers, collate_fn=collate_fn, pin_memory=True
      )

  # Define the model
  device = "cuda" if torch.cuda.is_available() else "cpu"
  if args.cpu:
    device = "cpu"
  model, loss_fn, optim_func = initialize_model(args.model, 3, device = device)
  print("Running training with the {}".format(args.loader))
  if args.benchmark:
    args.epochs = 0
  run_training_get_results(
      model, trainloader, testloader, optim_func, loss_fn, args.epochs, device, target_accuracy = args.accuracy
  )

main()