"""
This has been adjusted so that it works for images of any shape. MNIST has images with dimension
(1, 28, 28) (784), while CIFAR-10 has images with dimension (3, 32, 32).

The CIFAR-10 images are only slightly larger than the MNIST images 32x32x3 (3072 bytes)
vs 28x28x1 (784 bytes), respectively. After running some tests, the load times were similar,
so we decided to use a subset of the ImageNet dataset also. We selected 10 classes of images
from the dataset (tench, English springer, cassette player, chain saw, church, French horn,
garbage truck, gas pump, golf ball, parachute.
This amounted to about 900 examples each for 8,937 total images we and resized each image to
256x256x3 (196,608 bytes).
"""
from __future__ import annotations
from threading import Lock

import random
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from ctypes import Union
from io import BytesIO
from pathlib import Path
from typing import Callable
from functools import partial

import boto3
import numpy as np
import torch
import torchvision
from PIL import Image
from torch.utils.data import Dataset
from torch.utils.data._utils import collate
from torchvision.transforms import functional as F

import go_bindings
import logging_utils

LOGGER = logging_utils.initialize_logger(add_handler=True)

GO_LIB = go_bindings.load_go_lib("./ecClient.so")
GO_LIB.initializeVars()


class DatasetDisk(Dataset):
    """Simulates having to load each data point from disk every call."""

    def __init__(self, data_path: str, s3_bucket: str, label_idx: int):
        self.s3_client = boto3.client("s3")
        self.download_from_s3(s3_bucket, data_path)
        dataset_path = Path(data_path)  # path to where the data should be stored on Disk
        filenames = list(dataset_path.rglob("*.png"))
        filenames.extend(list(dataset_path.rglob("*.jpg")))
        self.filepaths = sorted(filenames, key=lambda filename: filename.stem)
        random.shuffle(self.filepaths)
        self.label_idx = label_idx

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = self.filepaths[idx].stem.split("_")[self.label_idx]
        pil_img = Image.open(self.filepaths[idx])
        img_tensor = F.pil_to_tensor(pil_img)
        return img_tensor, int(label)

    def download_from_s3(self, s3_path: str, local_path: str):
        """
        First need to download all images from S3 to Disk to use for training.
        """
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        filenames = []
        for page in paginator.paginate(Bucket=s3_path):
            for content in page.get("Contents"):
                filenames.append(content["Key"])
        partial_dl = partial(self.download_file, local_path, s3_path)
        LOGGER.info("Downloading data from S3 to Disk")
        with ThreadPoolExecutor(max_workers=64) as executor:
            futures = [executor.submit(partial_dl, fname) for fname in filenames]
            _ = [future.result() for future in as_completed(futures)]
        LOGGER.info("Download is complete")

    def download_file(self, output_dir: str, bucket_name: str, file_name: str):
        self.s3_client.download_file(bucket_name, file_name, f"{output_dir}/{file_name}")


class DatasetS3(Dataset):
    """Simulates having to load each data point from S3 every call."""

    def __init__(self, bucket_name: str, label_idx: int, channels: bool, testing: bool = False):
        self.label_idx = label_idx
        self.channels = channels
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name
        paginator = self.s3_client.get_paginator("list_objects_v2")
        filenames = []
        for page in paginator.paginate(Bucket=bucket_name):
            for content in page.get("Contents"):
                filenames.append(Path(content["Key"]))
        if "cifar" in bucket_name:
            test_fnames = self.get_test_fnames()
            if testing:
                filenames = list(test_fnames)
            else:
                filenames = list(set(filenames).difference(test_fnames))
        self.filepaths = sorted(filenames, key=lambda filename: filename.stem)
        random.shuffle(self.filepaths)

    @staticmethod
    def get_test_fnames():
        with open("cifar_test_fnames.txt") as f:
            test_fnames = set([Path(fname.strip()) for fname in f.readlines()])
        return test_fnames

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = self.filepaths[idx].stem.split("_")[self.label_idx]
        s3_png = self.s3_client.get_object(Bucket=self.bucket_name, Key=str(self.filepaths[idx]))
        img_bytes = s3_png["Body"].read()

        pil_img = Image.open(BytesIO(img_bytes))
        img_tensor = F.pil_to_tensor(pil_img)
        return img_tensor, int(label)


class BaseDataLoader(ABC):
    def __init__(
        self,
        dataset: Union[DatasetS3, DatasetDisk],
        dataset_name: str,
        img_dims: tuple[int, int, int],
        image_dtype: go_bindings.NumpyDtype,
        batch_size: int,
        collate_fn: Callable,
    ):
        self.index = 0
        self.dataset = dataset
        self.batch_size = batch_size
        self.collate_fn = collate_fn
        self.img_dims = img_dims
        self.data_type = image_dtype
        self.labels_cache = {}
        self.load_times = []
        self.total_samples = 0
        self.dataset_name = dataset_name
        self.lock = Lock()
        if img_dims[0] == 1:
            self.transform = torchvision.transforms.Compose([
                torchvision.transforms.Normalize((0.485), (0.229)),
            ])
        else:
            self.transform = torchvision.transforms.Compose([
                torchvision.transforms.Normalize((0.485, 0.456, 0.406), (0.229, 0.224, 0.225)),
            ])

    def __iter__(self):
        self.index = 0
        return self

    @abstractmethod
    def __next__(self):
        pass

    def get(self):
        curr_idx = self.increment_get()
        item = self.dataset[curr_idx]
        return item

    def increment_get(self) -> int:
        with self.lock:
            old_idx = self.index
            self.index += 1
            return old_idx

    def __len__(self):
        return len(self.dataset) // self.batch_size + 1


class DiskLoader(BaseDataLoader):
    def __init__(
        self,
        dataset: DatasetDisk,
        dataset_name: str,
        img_dims: tuple[int, int, int],
        image_dtype: go_bindings.NumpyDtype = np.uint8,
        batch_size: int = 64,
        collate_fn: Callable = collate.default_collate,
    ):
        super().__init__(dataset, dataset_name, img_dims, image_dtype, batch_size, collate_fn)

    def __next__(self):
        if self.index >= len(self.dataset):
            raise StopIteration
        batch_size = min(len(self.dataset) - self.index, self.batch_size)
        start_time = time.time()
        results = self.get_batch_threaded(batch_size)
        images, labels = self.collate_fn(results)
        images = self.transform(images.to(torch.float32).div(255))

        end_time = time.time()
        time_taken = end_time - start_time
        self.total_samples += batch_size
        self.load_times.append(time_taken)
        return images, labels

    def __str__(self):
        return "DiskDataset"

    def get_batch_threaded(self, batch_size: int):
        """
        This has been added so that all loading methods are done in parallel for comparison.
        """
        results = []
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(self.get) for _ in range(batch_size)]
            results = [future.result() for future in as_completed(futures)]
        return results


class S3Loader(BaseDataLoader):
    def __init__(
        self,
        dataset: DatasetS3,
        dataset_name: str,
        img_dims: tuple[int, int, int],
        image_dtype: go_bindings.NumpyDtype = np.uint8,
        batch_size: int = 64,
        collate_fn: Callable = collate.default_collate,
    ):
        super().__init__(dataset, dataset_name, img_dims, image_dtype, batch_size, collate_fn)

    def __next__(self):
        if self.index >= len(self.dataset):
            raise StopIteration
        batch_size = min(len(self.dataset) - self.index, self.batch_size)
        start_time = time.time()

        results = self.get_batch_threaded(batch_size)
        images, labels = self.collate_fn(results)
        images = self.transform(images.to(torch.float32).div(255))

        end_time = time.time()
        time_taken = end_time - start_time
        self.total_samples += batch_size
        self.load_times.append(time_taken)
        return images, labels

    def __str__(self):
        return "S3Dataset"

    def get_batch_threaded(self, batch_size: int):
        results = []
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(self.get) for _ in range(batch_size)]
            results = [future.result() for future in as_completed(futures)]
        return results


class InfiniCacheLoader(BaseDataLoader):
    """DataLoader specific to InfiniCache. Associates each batch of images with a key in the cache,
    rather than each image.
    """

    def __init__(
        self,
        dataset: DatasetS3,
        dataset_name: str,
        img_dims: tuple[int, int, int],
        image_dtype: go_bindings.NumpyDtype = np.uint8,
        batch_size: int = 64,
        collate_fn: Callable = collate.default_collate,
    ):
        super().__init__(dataset, dataset_name, img_dims, image_dtype, batch_size, collate_fn)
        self.base_keyname = f"{self.dataset_name}_{self.batch_size}_"
        self.initial_set_all_data()

    def __next__(self):
        if self.index >= len(self.dataset):
            raise StopIteration
        start_time = time.time()
        key = f"{self.base_keyname}_{self.index:05d}"
        batch_size = min(len(self.dataset) - self.index, self.batch_size)
        self.data_shape = (batch_size, *self.img_dims)

        try:
            np_arr = go_bindings.get_array_from_cache(GO_LIB, key, self.data_type, self.data_shape)
            images = np_arr.reshape(self.data_shape)
            images = torch.tensor(np_arr).reshape(self.data_shape)
            labels = self.labels_cache[key]
            self.index += self.batch_size
            data = (images.to(torch.float32), labels)

        except KeyError:
            results = self.get_batch_threaded(batch_size)
            images, labels = self.collate_fn(results)
            self.labels_cache[key] = labels
            go_bindings.set_array_in_cache(GO_LIB, key, np.array(images).astype(self.data_type))
            images = images.to(torch.float32).reshape(self.data_shape)
            data = (images, labels)
        end_time = time.time()
        time_taken = end_time - start_time
        self.total_samples += batch_size
        self.load_times.append(time_taken)

        return data

    def __str__(self):
        return "InfiniCacheDataset"

    def get_batch_threaded(self, batch_size: int):
        results = []
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(self.get) for _ in range(batch_size)]
            results = [future.result() for future in as_completed(futures)]
        return results

    def set_in_cache(self, batch_size: int, idx: int):
        idx *= batch_size
        key = f"{self.base_keyname}_{idx:05d}"
        results = self.get_batch_threaded(batch_size)
        images, labels = self.collate_fn(results)
        self.labels_cache[key] = labels
        go_bindings.set_array_in_cache(GO_LIB, key, np.array(images).astype(self.data_type))

    def initial_set_all_data(self):
        LOGGER.info("Loading data into InfiniCache in parallel")
        batch_sizes = [
            b_size
            for idx in range((len(self.dataset.filepaths) // self.batch_size) + 1)
            if (b_size := len(self.dataset.filepaths[idx * self.batch_size: idx * self.batch_size + self.batch_size]))
            > 0
        ]
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.set_in_cache, b_size, idx) for idx, b_size in enumerate(batch_sizes)]
            _ = [future.result() for future in as_completed(futures)]
            LOGGER.info("DONE with initial SET into InfiniCache")
        end_time = time.time()
        time_taken = end_time - start_time
        self.load_times.append(time_taken)
        LOGGER.info(
            "Finished Setting Data in InfiniCache. Total load time for %d samples is %.3f sec.",
            self.total_samples,
            time_taken,
        )
