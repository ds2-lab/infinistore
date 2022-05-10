import argparse
import torch

from pytorch_training import initialize_model, run_training_get_results
from infinicache_dataloaders import DatasetDisk, DatasetS3, DiskLoader, S3Loader, InfiniCacheLoader

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
  parser.add_argument("--cpu", action=argparse.BooleanOptionalAction, help="Using cpu for training.")
  parser.add_argument("--disk_source", action='store', type=str, help="Load dataset from disk and specifiy the path.", default="")
  parser.add_argument("--s3_source", action='store', type=str, help="Load dataset from S3 and specifiy the bucket.", default="cifar10-infinicache")
  parser.add_argument("--loader", action='store', type=str, help="Dataloader used, choosing from disk, s3, or infinicache.", default="disk")
  parser.add_argument("--model", action='store', type=str, help="Pretrained model, choosing from resnet, efficientnet or densenet.", default="")
  parser.add_argument("--batch", action='store', type=int, help="The batch size.", default=64)
  parser.add_argument("--epochs", action='store', type=int, help="Max epochs.", default=100)
  parser.add_argument("--accuracy", action='store', type=float, help="Target accuracy.", default=1.0)
  parser.add_argument("--benchmark", action=argparse.BooleanOptionalAction, help="Simply benchmark the pretrained model.")

  args, _ = parser.parse_known_args()

  # Define the dataset
  if args.disk_source != "":
    dataset = DatasetDisk(
        data_path = args.disk_sourc, s3_bucket = args.s3_source, label_idx=0
    )
  else:
    dataset = DatasetS3(
        args.s3_source, label_idx=0, channels=True
    )

  # Define the dataloader
  if args.dataset == "mnist":
    img_dims = (1, 28, 28)
  elif args.dataset == "cifar":
    img_dims = (3, 32, 32)
  else:
    img_dims = (3, 256, 256)

  # Define the dataloader
  if args.loader == "s3":
    dataloader = S3Loader(
      dataset, dataset_name=args.dataset, img_dims=img_dims, batch_size=args.batch
    )
  elif args.loader == "infinicache":
    dataloader = InfiniCacheLoader(
      dataset, dataset_name=args.dataset, img_dims=img_dims, batch_size=args.batch
    )
  else:
    dataloader = DiskLoader(
      dataset, dataset_name=args.dataset, img_dims=img_dims, batch_size=args.batch
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
      model, dataloader, optim_func, loss_fn, args.epochs, device, accuracy = args.accuracy
  )