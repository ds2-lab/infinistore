from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Optional, Union
from threading import Lock

import boto3
import numpy as np
import torch
import torchvision
from PIL import Image
from torch.utils.data import Dataset
from torchvision.transforms import functional as F

import go_bindings
import logging_utils

LOGGER = logging_utils.initialize_logger(add_handler=True)

GO_LIB = go_bindings.load_go_lib("./ecClient.so")
GO_LIB.initializeVars()


class LoadTimes:
    def __init__(self, name: str):
        self.name = name
        self.count = 0
        self.avg = 0
        self.sum = 0
        self.lock = Lock()

    def reset(self):
        with self.lock:
            self.num_loads = 0
            self.avg = 0
            self.sum = 0

    def update(self, val: Union[float, int]):
        with self.lock:
            self.sum += val
            self.count += 1
            self.avg = self.sum / self.count

    def __str__(self):
        return f"{self.name}: Average={self.avg:.03f}\tSum={self.sum:.03f}\tCount={self.count}"


class DatasetDisk(Dataset):
    """Simulates having to load each data point from disk every call."""

    def __init__(
        self,
        filepaths: list[str],
        label_idx: int,
        dataset_name: str,
        img_transform: Optional[torchvision.transforms.Compose] = None,
    ):
        self.filepaths = np.array(filepaths)
        self.label_idx = label_idx
        self.img_transform = img_transform
        self.total_samples = 0
        self.dataset_name = dataset_name

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = os.path.basename(self.filepaths[idx]).split(".")[0].split("_")[self.label_idx]
        pil_img = Image.open(self.filepaths[idx])
        img_tensor = F.pil_to_tensor(pil_img)
        if self.img_transform:
            img_tensor = self.img_transform(img_tensor.to(torch.float32).div(255))
        self.total_samples += 1

        return img_tensor, int(label)

    def __str__(self):
        return f"{self.dataset_name}_DatasetDisk"


class MiniObjDataset(Dataset):
    def __init__(
        self,
        bucket_name: str,
        label_idx: int,
        channels: bool,
        dataset_name: str,
        img_dims: tuple[int, int, int],
        obj_size: int = 8,
        img_transform: Optional[torchvision.transforms.Compose] = None,
    ):
        self.label_idx = label_idx
        self.channels = channels
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name
        paginator = self.s3_client.get_paginator("list_objects_v2")
        filenames = []
        labels = []
        for page in paginator.paginate(Bucket=bucket_name):
            for content in page.get("Contents"):
                filenames.append(content["Key"])
                labels.append(int(content["Key"].split(".")[0].split("_")[self.label_idx]))

        self.object_size = obj_size
        # Chunk the filenames into objects of size self.object_size where self.object_size is the
        # number of images.
        multiple_len = len(filenames) - (len(filenames) % self.object_size)
        filenames_arr = np.array(filenames[:multiple_len])
        assert len(filenames_arr) % self.object_size == 0
        labels_arr = np.array(labels[:multiple_len])

        # Needs to be a numpy array to avoid memory leaks with multiprocessing:
        #       https://pytorch.org/docs/stable/data.html#multi-process-data-loading
        self.chunked_fpaths = np.array(np.split(filenames_arr, (len(filenames_arr) // self.object_size)))
        self.chunked_labels = np.array(np.split(labels_arr, (len(labels_arr) // self.object_size)))

        self.base_keyname = f"{dataset_name}-{self.object_size}-"
        self.img_dims = img_dims
        self.data_type = np.uint8
        self.labels = np.ones(self.chunked_labels.shape, dtype=self.data_type)
        self.total_samples = 0
        self.img_transform = img_transform

    def __len__(self):
        return len(self.chunked_fpaths)

    def __getitem__(self, idx: int):
        num_samples = len(self.chunked_fpaths[idx])
        key = f"{self.base_keyname}-{idx:05d}"
        self.data_shape = (num_samples, *self.img_dims)

        try:
            np_arr = go_bindings.get_array_from_cache(GO_LIB, key, self.data_type, self.data_shape)
            images = torch.tensor(np_arr).reshape(self.data_shape)
            labels = torch.tensor(self.labels[idx])

        except KeyError:
            images, labels = self.get_s3_threaded(idx)
            self.labels[idx] = np.array(labels, dtype=self.data_type)
            go_bindings.set_array_in_cache(GO_LIB, key, np.array(images).astype(self.data_type))
            images = images.to(torch.float32).reshape(self.data_shape)

        if self.img_transform:
            images = self.img_transform(images.to(torch.float32).div(255))
        data = (images, labels)
        self.total_samples += num_samples
        return data

    def get_s3_threaded(self, idx: int):
        fpaths = self.chunked_fpaths[idx]
        # Returns 1-D tensor with number of labels
        labels = torch.tensor(self.chunked_labels[idx])
        with ThreadPoolExecutor(len(fpaths)) as executor:
            futures = [executor.submit(self.load_image, f) for f in fpaths]
            # Returns tensor of shape [object_size, num_channels, H, W]
            results = torch.stack([future.result() for future in as_completed(futures)])
        return results, labels

    def load_image(self, s3_prefix: str) -> torch.Tensor:
        s3_png = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_prefix)
        img_bytes = s3_png["Body"].read()
        pil_img = Image.open(BytesIO(img_bytes))
        img_tensor = F.pil_to_tensor(pil_img)
        return img_tensor

    def set_in_cache(self, idx: int):
        key = f"{self.base_keyname}-{idx:05d}"
        images, labels = self.get_s3_threaded(idx)
        self.labels[idx] = np.array(labels, dtype=self.data_type)
        go_bindings.set_array_in_cache(GO_LIB, key, np.array(images).astype(self.data_type))

    def initial_set_all_data(self):
        idxs = list(range(len(self.chunked_fpaths)))
        LOGGER.info("Loading data into InfiniCache in parallel")

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.set_in_cache, idx) for idx in idxs]
            _ = [future.result() for future in as_completed(futures)]
            LOGGER.info("DONE with initial SET into InfiniCache")
        end_time = time.time()
        time_taken = end_time - start_time

        LOGGER.info(
            "Finished Setting Data in InfiniCache. Total load time for %d samples is %.3f sec.",
            self.total_samples,
            time_taken,
        )

    def __str__(self):
        return f"{self.dataset_name}_MiniObjDataset"
