"""
Deep learning training cycle.
"""
from __future__ import annotations
import random

import time
from typing import Union
import numpy as np

import torch
import torch.nn as nn
from torch.utils.data import DataLoader

import cnn_models
import infinicache_dataloaders
import logging_utils

LOGGER = logging_utils.initialize_logger(True)
DATALOG = logging_utils.get_logger("datalog")

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
    train_dataloader: Union[
        infinicache_dataloaders.InfiniCacheLoader,
        infinicache_dataloaders.S3Loader,
        infinicache_dataloaders.DiskLoader,
        DataLoader,
    ],
    optim_func: torch.optim.Adam = None,
    loss_fn: nn.CrossEntropyLoss = None,
    num_epochs: int = 1,
    device: str = "cuda"
):
    num_batches = len(train_dataloader)

    validation = num_epochs == 0
    if validation:
        num_epochs = 1 

    loading_time = 0.0
    for epoch in range(num_epochs):
        iteration = 0
        running_loss = 0.0
        model.train(mode=not validation)
        history = None
        start_time = time.time()
    
        for idx, (images, labels) in enumerate(train_dataloader):
            loading_time += time.time() - start_time
            images = images.to(device)
            labels = labels.to(device)
        
            if not validation:
                logits, _ = model(images)

                loss = loss_fn(logits, labels)
                optim_func.zero_grad()
                loss.backward()
                optim_func.step()
                running_loss += float(loss.item())
            else:
                with torch.no_grad():
                    logits, _ = model(images)

            iteration += 1
            history = count_top_k_preds(logits, labels, history)

            if not idx % 100:
                print(
                    (
                        f"Epoch: {epoch+1:03d}/{num_epochs:03d} |"
                        f" Batch: {idx+1:03d}/{num_batches:03d} |"
                        f" Cost: {running_loss/iteration:.4f} |"
                        f" Elapsed: {time.time() - start_time:.3f} secs"
                    )
                )
                iteration = 0
                running_loss = 0.0

        top1, top5, total = history
        start_time = time.time()

    return loading_time, top1 / total, top5 / total

def compare_pred_vs_actual(logit_scores: torch.Tensor, labels: torch.Tensor, silent: bool = False):
    logit_scores = logit_scores.to("cpu")
    labels = labels.to("cpu")
    logit_preds = torch.argmax(logit_scores, axis=1)
    num_correct = torch.sum(logit_preds == labels)
    perc_correct = num_correct / labels.shape[0] * 100
    if not silent:
        print(f"Num correct is: {num_correct}/{labels.shape[0]} ({perc_correct}%)")
    return perc_correct

def count_top_k_preds(logit_scores: torch.Tensor, labels: torch.Tensor, history = None):
    top1, top5, total = 0.0, 0.0, 0.0
    if history is not None:
        top1, top5, total = history

    predictions = logit_scores.topk(5)[1].data.cpu().numpy()
    labels = labels.data.cpu().numpy()
    total += len(labels)
    for i in range(len(labels)):
        if labels[i] == predictions[i, 0]:
            top1 += 1
            top5 += 1
            continue
        
        for j in range(1, 4):
            if labels[i] == predictions[i, j]:
                top5 += 1
                break

    return top1, top5, total


def run_training_get_results(
    model: nn.Module,
    data_loader: DataLoader,
    validation_loader: DataLoader,
    optim_func: torch.optim,
    loss_fn: nn.CrossEntropyLoss,
    num_epochs: int,
    device: str,
    target_accuracy: float = 1.0,
):
    validation_loading_time = 0.0
    validation_time = 0.0
    training_loading_time = 0.0
    training_time = 0.0

    if validation_loader is not None:
        validation_start = time.time()
        validation_loading_time, accuracy, top5 = training_cycle(model, validation_loader, num_epochs=0, device=device)
        validation_time = time.time() - validation_start
        LOGGER.info("Pretrained top-1 accuracy: %.3f, top-5 accuracy %.3f", accuracy * 100, top5 * 100)
        DATALOG.info("%d,%d,%f,%f,%f,%f,%f", 1, 0, validation_start, validation_loading_time, validation_time, accuracy, top5)

    for epoch in range(num_epochs):
        epoch_start = time.time()
        loading_time, accuracy, top5 = training_cycle(model, data_loader, optim_func, loss_fn, 1, device=device)
        training_time += time.time() - epoch_start
        training_loading_time += loading_time
        LOGGER.info(
            "[Epoch %3d] Training for %s with %d samples, data loading time %.3f sec, training time %.3f sec, top accuracies %.3f, %.3f.",
            epoch + 1,
            str(data_loader),
            data_loader.total_samples if hasattr(data_loader, "total_samples") else data_loader.dataset.total_samples,
            training_loading_time,
            training_time,
            accuracy * 100,
            top5 * 100,
        )
        DATALOG.info("%d,%d,%f,%f,%f,%f,%f", 0, epoch + 1, epoch_start, training_loading_time, training_time, accuracy, top5)

        if validation_loader is not None:
            validation_start = time.time()
            loading_time, accuracy, top5 = training_cycle(model, validation_loader, num_epochs=0, device=device)
            validation_time += time.time() - validation_start
            validation_loading_time += loading_time
            LOGGER.info("[Epoch %3d] Validation top-1 accuracy: %.3f, top-5 accuracy %.3f", epoch + 1, accuracy * 100, top5 * 100)
            DATALOG.info("%d,%d,%f,%f,%f,%f,%f", 1, epoch + 1, validation_start, validation_loading_time, validation_time, accuracy, top5)

            if accuracy >= target_accuracy:
                LOGGER.info("Accuracy reached.")
                break

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


def get_dataloader_times(data_loader: DataLoader):
    idx = len(data_loader)
    start_time = time.time()
    for i, _ in enumerate(data_loader):
        if not i % 100:
            print(i)
        if i > 200:
            break
    end_time = time.time()
    print(f"Time taken: {end_time - start_time}.")
    print(f"Time taken per iter: {(end_time - start_time) / idx}.")


if __name__ == "__main__":
    LOGGER.info("TRAINING STARTED")

    #  MNIST ####################################################
    mnist_dataset_ebs = infinicache_dataloaders.DatasetDisk(
        data_path="/home/ubuntu/mnist_png", s3_bucket="mnist-infinicache", label_idx=-1
    )
    mnist_dataset_efs = infinicache_dataloaders.DatasetDisk(
        data_path="/home/ubuntu/efs/mnist_png", s3_bucket="mnist-infinicache", label_idx=-1
    )
    mnist_dataset_s3 = infinicache_dataloaders.DatasetS3(
        "mnist-infinicache", label_idx=-1, channels=False
    )
    mnist_dataset_cache = infinicache_dataloaders.DatasetS3(
        "mnist-infinicache", label_idx=-1, channels=False
    )

    mnist_dataloader_cache = infinicache_dataloaders.InfiniCacheLoader(
        mnist_dataset_cache, dataset_name="mnist", img_dims=(1, 28, 28), batch_size=64
    )
    mnist_dataloader_ebs = infinicache_dataloaders.DiskLoader(
        mnist_dataset_ebs, dataset_name="mnist", img_dims=(1, 28, 28), batch_size=64
    )
    mnist_dataloader_efs = infinicache_dataloaders.DiskLoader(
        mnist_dataset_efs, dataset_name="mnist", img_dims=(1, 28, 28), batch_size=64
    )
    mnist_dataloader_s3 = infinicache_dataloaders.S3Loader(
        mnist_dataset_s3, dataset_name="mnist", img_dims=(1, 28, 28), batch_size=64
    )

    model, loss_fn, optim_func = initialize_model("basic", num_channels=1)
    print("Running training with the EBS dataloader")
    run_training_get_results(model, mnist_dataloader_ebs, optim_func, loss_fn, NUM_EPOCHS, DEVICE)

    model, loss_fn, optim_func = initialize_model("basic", num_channels=1)
    print("Running training with the EFS dataloader")
    run_training_get_results(model, mnist_dataloader_efs, optim_func, loss_fn, NUM_EPOCHS, DEVICE)

    model, loss_fn, optim_func = initialize_model("basic", num_channels=1)
    print("Running training with the cache dataloader")
    run_training_get_results(model, mnist_dataloader_cache, optim_func, loss_fn, NUM_EPOCHS, DEVICE)

    model, loss_fn, optim_func = initialize_model("basic", num_channels=1)
    print("Running training with the S3 dataloader")
    run_training_get_results(model, mnist_dataloader_s3, optim_func, loss_fn, NUM_EPOCHS, DEVICE)

    #  IMAGENET ####################################################
    imagenet_dataset_ebs = infinicache_dataloaders.DatasetDisk(
        data_path="/home/ubuntu/imagenet_png", s3_bucket="imagenet-infinicache-png", label_idx=0
    )
    imagenet_dataset_efs = infinicache_dataloaders.DatasetDisk(
        data_path="/home/ubuntu/efs/imagenet_png", s3_bucket="imagenet-infinicache-png", label_idx=0
    )
    imagenet_dataset_s3 = infinicache_dataloaders.DatasetS3(
        "imagenet-infinicache-png", label_idx=0, channels=True
    )
    imagenet_dataset_cache = infinicache_dataloaders.DatasetS3(
        "imagenet-infinicache-png", label_idx=0, channels=True
    )

    imagenet_dataloader_ebs = infinicache_dataloaders.DiskLoader(
        imagenet_dataset_ebs, dataset_name="imagenet", img_dims=(3, 256, 256), batch_size=64
    )
    imagenet_dataloader_efs = infinicache_dataloaders.DiskLoader(
        imagenet_dataset_efs, dataset_name="imagenet", img_dims=(3, 256, 256), batch_size=64
    )
    imagenet_dataloader_s3 = infinicache_dataloaders.S3Loader(
        imagenet_dataset_s3, dataset_name="imagenet", img_dims=(3, 256, 256), batch_size=64
    )
    imagenet_dataloader_cache = infinicache_dataloaders.InfiniCacheLoader(
        imagenet_dataset_cache, dataset_name="imagenet", img_dims=(3, 256, 256), batch_size=64
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the EBS dataloader")
    run_training_get_results(
        model, imagenet_dataloader_ebs, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the EFS dataloader")
    run_training_get_results(
        model, imagenet_dataloader_efs, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the Cache dataloader")
    run_training_get_results(
        model, imagenet_dataloader_cache, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the S3 dataloader")
    run_training_get_results(
        model, imagenet_dataloader_s3, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    #  CIFAR ####################################################
    cifar_dataset_ebs = infinicache_dataloaders.DatasetDisk(
        data_path="/home/ubuntu/cifar_images", s3_bucket="cifar10-infinicache", label_idx=0
    )
    cifar_dataset_efs = infinicache_dataloaders.DatasetDisk(
        data_path="/home/ubuntu/efs/cifar_images", s3_bucket="cifar10-infinicache", label_idx=0
    )
    cifar_dataset_s3 = infinicache_dataloaders.DatasetS3(
        "cifar10-infinicache", label_idx=0, channels=True
    )
    cifar_dataset_cache = infinicache_dataloaders.DatasetS3(
        "cifar10-infinicache", label_idx=0, channels=True
    )

    cifar_dataloader_ebs = infinicache_dataloaders.DiskLoader(
        cifar_dataset_ebs, dataset_name="cifar", img_dims=(3, 32, 32), batch_size=64
    )
    cifar_dataloader_efs = infinicache_dataloaders.DiskLoader(
        cifar_dataset_efs, dataset_name="cifar", img_dims=(3, 32, 32), batch_size=64
    )
    cifar_dataloader_s3 = infinicache_dataloaders.S3Loader(
        cifar_dataset_s3, dataset_name="cifar", img_dims=(3, 32, 32), batch_size=64
    )
    cifar_dataloader_cache = infinicache_dataloaders.InfiniCacheLoader(
        cifar_dataset_cache, dataset_name="cifar", img_dims=(3, 32, 32), batch_size=64
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the EBS dataloader")
    run_training_get_results(
        model, cifar_dataloader_ebs, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the EFS dataloader")
    run_training_get_results(
        model, cifar_dataloader_efs, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the Cache dataloader")
    run_training_get_results(
        model, cifar_dataloader_cache, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    model, loss_fn, optim_func = initialize_model("basic", 3)
    print("Running training with the S3 dataloader")
    run_training_get_results(
        model, cifar_dataloader_s3, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )
