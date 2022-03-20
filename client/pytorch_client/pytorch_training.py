"""
TODO: Change the slow cache checking time if a key isn't present.
TODO: Look into the set_array_in_cache function. It's too slow to add keys. Async maybe?
TODO: Change the manual training cycle to an abstraction within PyTorch Lightning.
TODO: Have a test set holdout.
"""
from __future__ import annotations

import time

import torch
import torch.nn as nn
import pytorch_process
from torch.utils.data import DataLoader
from torchvision import models

NUM_EPOCHS = 3
NUM_CLASSES = 10  # NUM DIGITS
NUM_CHANNELS = 1  # MNIST images are grayscale
DEVICE = "cuda:0"
LEARNING_RATE = 1e-3


class Resnet50(nn.Module):
    def __init__(self, num_channels):
        """We need to adjust the input size expected for the ResNet50 Model based on the image
        shapes.
        """
        super().__init__()
        self.resnet_model = models.resnet50(pretrained=True)
        fc_features = self.resnet_model.fc.in_features
        self.resnet_model.fc = nn.Linear(fc_features, 10)
        self.resnet_model.conv1 = nn.Conv2d(
            num_channels, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
        )
        self.classification_layer = nn.Softmax(dim=1)

    def forward(self, data_inputs):
        logits = self.resnet_model(data_inputs)
        probs = self.classification_layer(logits)
        return logits, probs


def training_cycle(
    model: Resnet50,
    train_dataloader: DataLoader,
    optim_func: torch.optim.Adam,
    loss_fn: nn.CrossEntropyLoss,
    num_epochs: int = 1,
    device: str = "cuda:0",
):
    start_time = time.time()
    num_batches = len(train_dataloader)
    for epoch in range(num_epochs):
        iteration = 0
        running_loss = 0.0
        model.train()
        for idx, (images, labels) in enumerate(train_dataloader):
            images = images.to(device)
            print(images.shape)
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
                iteration = 0
                running_loss = 0.0
            if idx > 2:
                break
    end_time = time.time()
    print(f"Time taken: {end_time - start_time}")


def compare_pred_vs_actual(logit_scores: torch.Tensor, labels: torch.Tensor):
    logit_scores = logit_scores.to("cpu")
    labels = labels.to("cpu")
    logit_preds = torch.argmax(logit_scores, axis=1)
    num_correct = torch.sum(logit_preds == labels)
    perc_correct = num_correct / labels.shape[0] * 100
    print(f"Num correct is: {num_correct}/{labels.shape[0]} ({perc_correct}%)")


def run_training_get_results(
    model: Resnet50,
    data_loader: DataLoader,
    optim_func: torch.optim,
    loss_fn: nn.CrossEntropyLoss,
    num_epochs: int,
    device: str,
):
    training_cycle(model, data_loader, optim_func, loss_fn, num_epochs, device)
    sample_loader = next(iter(data_loader))
    sample_loader_data = sample_loader[0].to(torch.float32).to(device)
    sample_loader_labels = sample_loader[1]
    resnet_result = model(sample_loader_data)
    compare_pred_vs_actual(resnet_result[0], sample_loader_labels)


def initialize_model() -> tuple[Resnet50, nn.CrossEntropyLoss, torch.optim.Adam]:
    resnet50_model = Resnet50(NUM_CHANNELS)
    resnet50_model = resnet50_model.to(DEVICE)
    resnet50_model.train()
    loss_fn = nn.CrossEntropyLoss()
    optim_func = torch.optim.Adam(resnet50_model.parameters(), lr=LEARNING_RATE)
    return resnet50_model, loss_fn, optim_func


if __name__ == "__main__":

    mnist_dataset_cache = pytorch_process.MnistDatasetCache("/home/ubuntu/mnist_png")
    mnist_dataset_disk = pytorch_process.MnistDatasetDisk("/home/ubuntu/mnist_png")
    mnist_dataloader_cache = DataLoader(mnist_dataset_cache, batch_size=50)
    mnist_dataloader_disk = DataLoader(mnist_dataset_disk, batch_size=50)

    resnet50_model, loss_fn, optim_func = initialize_model()
    print("Running training with the disk dataloader")
    run_training_get_results(
        resnet50_model, mnist_dataloader_disk, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )

    resnet50_model, loss_fn, optim_func = initialize_model()
    print("Running training with the cache dataloader")
    run_training_get_results(
        resnet50_model, mnist_dataloader_cache, optim_func, loss_fn, NUM_EPOCHS, DEVICE
    )
