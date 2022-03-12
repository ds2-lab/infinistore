"""
Working with the MNIST dataset. Testing out using a PyTorch Dataset/DataLoader. If an image isn't
in the cache, it's loaded from disk and then stored in the cache with its index as the key.
"""
import time
from pathlib import Path

import numpy as np
import torch
import torchvision
from torch.utils.data import DataLoader, Dataset

import pytorch_func


def test_mnist_data():
    mnist_dataset = torchvision.datasets.MNIST("/home/ubuntu")

    # Put data in cache
    key = "mnist_1"
    input_data = np.array(mnist_dataset.data[0])

    go_lib = pytorch_func.load_go_lib("./ecClient.so")

    pytorch_func.set_array_in_cache(go_lib, key, input_data)
    cache_arr = pytorch_func.get_array_from_cache(go_lib, key, input_data.dtype, input_data.shape)
    print("Array from cache shape: ", cache_arr.shape)
    print("Array from cache: ", cache_arr)
    assert np.sum(cache_arr == input_data) == 784


def test_mnist_from_disk(path_to_mnist_images: str):
    ds = MnistDataset(path_to_mnist_images)
    print("Getting sample from disk")
    start_time = time.time()
    ds[1]
    end_time = time.time()
    print(end_time - start_time)

    print("Getting sample from cache")
    start_time = time.time()
    ds[1]
    end_time = time.time()
    print(end_time - start_time)

    mnist_dataloader = DataLoader(ds, batch_size=20)
    images, labels = next(iter(mnist_dataloader))
    print(images[0], labels[0])


def save_mnist_images(root_path: str, dir_to_save_images: str):
    mnist_dataset = torchvision.datasets.MNIST(root_path, download=True)
    for idx, (img, label) in enumerate(mnist_dataset):
        img.save(f"{dir_to_save_images}/{idx:05d}_{label}.jpg")


class MnistDataset(Dataset):
    """Simulates having to load each data point from disk every call."""

    def __init__(self, mnist_path: str):
        self.go_lib = pytorch_func.load_go_lib("./ecClient.so")
        self.data_shape = (28, 28)
        self.data_type = np.uint8

        mnist_dataset_path = Path(mnist_path)
        filenames = list(mnist_dataset_path.rglob("*.jpg"))
        self.filepaths = sorted(filenames, key=lambda filename: int(filename.stem.split("_")[0]))

    def __len__(self):
        return len(self.filepaths)

    def __getitem__(self, idx: int):
        label = self.filepaths[idx].stem.split("_")[-1]
        key = f"mnist_{idx:05d}"
        try:
            np_arr = pytorch_func.get_array_from_cache(
                self.go_lib, key, self.data_type, self.data_shape
            )
            print("Found in cache")
            img = torch.tensor(np_arr)
        except KeyError:
            print("Loading from disk")
            img = torchvision.io.read_image(str(self.filepaths[idx]))
            pytorch_func.set_array_in_cache(self.go_lib, key, np.array(img))
        return img, int(label)


if __name__ == "__main__":
    test_mnist_data()
    test_mnist_from_disk("/home/ubuntu/mnist_images")
