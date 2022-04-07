"""
Working with the MNIST dataset. Testing out using a PyTorch Dataset/DataLoader. If an image isn't
in the cache, it's loaded from disk and then stored in the cache with its index as the key.
"""
import random
import time

import numpy as np
import torchvision
from torch.utils.data import DataLoader

from infinicache_dataloaders import MnistDatasetCache
import go_bindings

GO_LIB = go_bindings.load_go_lib("./ecClient.so")
GO_LIB.initializeVars()


def test_mnist_data():
    mnist_dataset = torchvision.datasets.MNIST("/home/ubuntu")

    # Put data in cache
    key = "mnist_1"
    input_data = np.array(mnist_dataset.data[0])

    go_bindings.set_array_in_cache(GO_LIB, key, input_data)
    cache_arr = go_bindings.get_array_from_cache(GO_LIB, key, input_data.dtype, input_data.shape)
    print("Array from cache shape: ", cache_arr.shape)
    print("Array from cache: ", cache_arr)
    assert np.sum(cache_arr == input_data) == 784


def test_mnist_from_disk(path_to_mnist_images: str):
    ds = MnistDatasetCache(path_to_mnist_images)
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
        img.save(f"{dir_to_save_images}/{idx:05d}_{label}.png")


if __name__ == "__main__":
    sample_path = "/home/ubuntu/mnist_png/05454_9.png"
    mnist_dataset = torchvision.datasets.MNIST("/home/ubuntu")

    # Put data in cache
    key = f"mnist_{random.randint(1, 1000)}"
    input_data = np.array(mnist_dataset.data[0])

    print("LIBRARY LOADED")

    print("READING FROM MNIST_DATASET")
    go_bindings.set_array_in_cache(GO_LIB, key, input_data)
    cache_arr = go_bindings.get_array_from_cache(GO_LIB, key, input_data.dtype, input_data.shape)
    print("Array from cache shape: ", cache_arr.shape)
    print("Array from cache: ", cache_arr)
    assert np.sum(cache_arr == input_data) == 784
    print("DONE READING FROM MNIST_DATASET")

    print("READING FROM DISK")
    key = f"mnist_{random.randint(1, 1000)}"
    img = torchvision.io.read_image(sample_path)
    img_np = np.array(img.squeeze(0))
    print(img.squeeze(0).shape)

    go_bindings.set_array_in_cache(GO_LIB, key, np.array(img))
    cache_arr = go_bindings.get_array_from_cache(GO_LIB, key, input_data.dtype, input_data.shape)
    print("Array from cache shape: ", cache_arr.shape)
    print("Array from cache: ", cache_arr)
    assert np.sum(cache_arr == np.array(img)) == 784
    print("DONE READING FROM DISK")

    mnist_dataset_cache = MnistDatasetCache("/home/ubuntu/mnist_png")
    mnist_dataset_sample = mnist_dataset_cache[0]
