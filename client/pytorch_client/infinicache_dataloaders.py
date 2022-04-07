"""
Working with the MNIST dataset. Testing out using a PyTorch Dataset/DataLoader. If an image isn't
in the cache, it's loaded from disk and then stored in the cache with its index as the key.
"""
from io import BytesIO
from pathlib import Path

import boto3
import numpy as np
import torch
import torchvision
from PIL import Image
from torch.utils.data import Dataset

import go_bindings

GO_LIB = go_bindings.load_go_lib("./ecClient.so")
GO_LIB.initializeVars()


class MnistDatasetCache(Dataset):
    """First checks InfiniCache for the key. If it doesn't exist, then it pulls the file from
    disk."""

    def __init__(self, mnist_path: str):
        self.data_shape = (28, 28)
        self.data_type = np.uint8
        self.keys = set()

        mnist_dataset_path = Path(mnist_path)
        filenames = list(mnist_dataset_path.rglob("*.png"))
        self.filepaths = sorted(filenames, key=lambda filename: int(filename.stem.split("_")[0]))

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = self.filepaths[idx].stem.split("_")[-1]
        key = f"mnist_{idx:05d}"
        try:
            np_arr = go_bindings.get_array_from_cache(GO_LIB, key, self.data_type, self.data_shape)
            img = torch.tensor(np_arr).reshape(1, 28, 28)
            assert img.shape == (1, 28, 28)

        except KeyError:
            img = torchvision.io.read_image(str(self.filepaths[idx])).reshape(1, 28, 28)
            assert img.shape == (1, 28, 28)
            go_bindings.set_array_in_cache(GO_LIB, key, np.array(img))
            self.keys.add(key)

        return img.to(torch.float32), int(label)


class MnistDatasetDisk(Dataset):
    """Simulates having to load each data point from disk every call."""

    def __init__(self, mnist_path: str):
        mnist_dataset_path = Path(mnist_path)
        filenames = list(mnist_dataset_path.rglob("*.png"))
        self.filepaths = sorted(filenames, key=lambda filename: int(filename.stem.split("_")[0]))

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = self.filepaths[idx].stem.split("_")[-1]
        img = torchvision.io.read_image(str(self.filepaths[idx]))
        return img.to(torch.float32), int(label)


class MnistDatasetS3(Dataset):
    """Simulates having to load each data point from S3 every call."""

    def __init__(self, bucket_name: str):
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name
        paginator = self.s3_client.get_paginator("list_objects_v2")
        filenames = []
        for page in paginator.paginate(Bucket=bucket_name):
            for content in page.get("Contents"):
                filenames.append(content["Key"])
        self.filepaths = sorted(filenames, key=lambda filename: int(filename.split("_")[0]))

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = Path(self.filepaths[idx]).stem.split("_")[-1]
        s3_png = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.filepaths[idx])
        img_bytes = s3_png["Body"].read()

        img = np.array(Image.open(BytesIO(img_bytes)))
        img_tensor = torch.from_numpy(img)
        return img_tensor.to(torch.float32), int(label)
