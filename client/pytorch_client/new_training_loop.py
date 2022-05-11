"""
Deep learning training cycle.
"""
from __future__ import annotations

import random
import time

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter

import cnn_models
import logging_utils
import updated_datasets
import utils

WRITER = SummaryWriter()
LOGGER = logging_utils.initialize_logger()

SEED = 1234
NUM_EPOCHS = 10
DEVICE = "cuda:0"
LEARNING_RATE = 1e-3

random.seed(SEED)
torch.manual_seed(SEED)
random.seed(SEED)
np.random.seed(SEED)


def training_cycle(
    model: nn.Module,
    train_dataloader: DataLoader,
    test_dataloader: DataLoader,
    optim_func: torch.optim.Adam,
    loss_fn: nn.CrossEntropyLoss,
    num_epochs: int = 1,
    device: str = "cuda:0",
):
    start_time = time.time()
    num_batches = len(train_dataloader)
    train_dl_times = updated_datasets.LoadTimes("TrainTimer")
    test_dl_times = updated_datasets.LoadTimes("TestTimer")

    for epoch in range(num_epochs):
        iteration = 0
        running_loss = 0.0
        model.train()

        train_load = time.time()
        for idx, (images, labels) in enumerate(train_dataloader):
            train_dl_times.update(time.time() - train_load)
            images = images.to(device)
            labels = labels.to(device)
            logits, _ = model(images)
            loss = loss_fn(logits, labels)

            iteration += 1
            optim_func.zero_grad()
            loss.backward()
            optim_func.step()
            running_loss += float(loss.item())

            if not idx % 100:
                print(
                    (
                        f"Epoch: {epoch+1:03d}/{num_epochs:03d} |"
                        f" Batch: {idx+1:03d}/{num_batches:03d} |"
                        f" Cost: {running_loss/iteration:.4f}"
                    )
                )
                WRITER.add_scalar("Loss/Train", running_loss / iteration, epoch * len(train_dataloader) + idx)
                WRITER.add_scalar("Average Load Time/Train", train_dl_times.avg, epoch * len(train_dataloader) + idx)
                WRITER.add_scalar("Sum Load Time/Train", train_dl_times.sum, epoch * len(train_dataloader) + idx)

                iteration = 0
                running_loss = 0.0
            train_load = time.time()

        total_time_taken = sum(train_dataloader.dataset.load_times)
        print("total time", total_time_taken)
        LOGGER.info(
            "Finished Epoch %d for %s. Total load time for %d samples is %.3f sec.",
            epoch + 1,
            str(train_dataloader.dataset),
            train_dl_times.count,
            train_dl_times.sum,
        )

        LOGGER.info("Running Testing:")
        with torch.no_grad():
            num_correct = 0
            test_samples = 0
            test_load = time.time()
            for idx, (images, labels) in enumerate(test_dataloader):
                test_dl_times.update(time.time() - test_load)

                images = images.to(device)
                labels = labels.to(device)
                logits, _ = model(images)
                logit_preds = torch.argmax(logits, axis=1)
                num_correct += torch.sum(logit_preds == labels)
                test_samples += labels.shape[0]
                test_load = time.time()

            total_time_taken = sum(test_dataloader.dataset.load_times)
            print("total time", total_time_taken)

            perc_correct = num_correct / test_samples * 100

            LOGGER.info(
                "Finished Testing Epoch %d for %s. Total load time for %d iterations is %.3f sec.",
                epoch + 1,
                str(test_dataloader.dataset),
                test_dl_times.count,
                test_dl_times.sum,
            )
            LOGGER.info(
                "Test results: %d / %d = %f",
                num_correct,
                test_samples,
                perc_correct,
            )
            WRITER.add_scalar("Accuracy/Test", perc_correct, epoch)
            WRITER.add_scalar("Average Load Time/Test", test_dl_times.avg, epoch)
            WRITER.add_scalar("Sum Load Time/Test", test_dl_times.sum, epoch)

    end_time = time.time()
    print(f"Training Time taken: {end_time - start_time}")


def initialize_model(
    model_type: str, num_channels: int, device: str = "cuda:0"
) -> tuple[nn.Module, nn.CrossEntropyLoss, torch.optim.Adam]:
    if model_type == "resnet":
        print("Initializing Resnet50 model")
        model = cnn_models.Resnet50(num_channels)
    elif model_type == "efficientnet":
        print("Initializing EfficientNetB4 model")
        model = cnn_models.EfficientNetB4(num_channels)
    elif model_type == "densenet":
        print("Initializing DenseNet161 model")
        model = cnn_models.DenseNet161(num_channels)
    else:
        print("Initializing BasicCNN model")
        model = cnn_models.BasicCNN(num_channels)

    model = model.to(device)
    model.train()
    loss_fn = nn.CrossEntropyLoss()
    optim_func = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
    return model, loss_fn, optim_func


if __name__ == "__main__":
    LOGGER.info("TRAINING STARTED")

    # CIFAR DISK
    train_cifar, test_cifar = utils.split_cifar_data(
        "/home/ubuntu/cifar_images",
        "/home/ubuntu/ssh_repo/LambdaObjectstore/client/pytorch_client/cifar_test_fnames.txt",
    )

    normalize_cifar = utils.normalize_image(True)

    train_ds = updated_datasets.DatasetDisk(train_cifar, 0, dataset_name="CIFAR-10", img_transform=normalize_cifar)
    test_ds = updated_datasets.DatasetDisk(test_cifar, 0, dataset_name="CIFAR-10", img_transform=normalize_cifar)

    train_dataloader = DataLoader(train_ds, batch_size=32, shuffle=True, num_workers=4, pin_memory=True)
    test_dataloader = DataLoader(test_ds, batch_size=32, shuffle=True, num_workers=4, pin_memory=True)

    model, loss_fn, optim_func = initialize_model("resnet", num_channels=3)
    print("Running training with the DISk dataloader")
    training_cycle(model, train_dataloader, test_dataloader, optim_func, loss_fn, 3, "cuda:0")

    # CIFAR INFINICACHE
    train_infini = updated_datasets.MiniObjDataset(
        "infinicache-cifar-train",
        label_idx=0,
        channels=True,
        dataset_name="CIFAR-10",
        img_dims=(3, 32, 32),
        obj_size=16,
        img_transform=normalize_cifar,
    )

    test_infini = updated_datasets.MiniObjDataset(
        "infinicache-cifar-test",
        label_idx=0,
        channels=True,
        dataset_name="CIFAR-10",
        img_dims=(3, 32, 32),
        obj_size=16,
        img_transform=normalize_cifar,
    )

    train_infini.initial_set_all_data()
    test_infini.initial_set_all_data()

    # batch_size input should be desired batch size divided by object size
    # E.g., If a batch size of 32 is desired and the object size is 8, then batch_size=4 is the input
    train_infini_dataloader = DataLoader(
        train_infini, batch_size=4, shuffle=True, num_workers=0, collate_fn=utils.infinicache_collate, pin_memory=True
    )

    test_infini_dataloader = DataLoader(
        test_infini, batch_size=4, shuffle=True, num_workers=0, collate_fn=utils.infinicache_collate, pin_memory=True
    )

    model, loss_fn, optim_func = initialize_model("resnet", num_channels=3)
    print("Running training with the InfiniCache dataloader")
    training_cycle(model, train_infini_dataloader, test_infini_dataloader, optim_func, loss_fn, 3, "cuda:0")
