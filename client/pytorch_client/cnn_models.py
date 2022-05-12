import torch
import torch.nn as nn
from torchvision import models


class Resnet50(nn.Module):
    def __init__(self, num_channels, num_classes):
        """We need to adjust the input size expected for the ResNet50 Model based on the image
        shapes.
        """
        super().__init__()
        self.resnet_model = models.resnet50(pretrained=True)
        self.adapt_pool = nn.AdaptiveAvgPool2d((28, 28))
        fc_features = self.resnet_model.fc.in_features
        self.resnet_model.fc = nn.Linear(fc_features, num_classes)
        self.resnet_model.conv1 = nn.Conv2d(
            num_channels, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
        )
        self.classification_layer = nn.Softmax(dim=1)

    def forward(self, data_inputs):
        x = self.adapt_pool(data_inputs)
        logits = self.resnet_model(x)
        probs = self.classification_layer(logits)
        return logits, probs


class BasicCNN(nn.Module):
    def __init__(self, num_channels, num_classes):
        super().__init__()
        self.adapt_pool = nn.AdaptiveAvgPool2d((28, 28))
        self.first_layer = nn.Conv2d(num_channels, 32, 3)
        self.second_layer = nn.Conv2d(32, 16, 3)
        self.fc_layer = nn.Linear(5 * 5 * 16, num_classes)
        self.classification_layer = nn.Softmax(dim=1)

        self.max_pool = nn.MaxPool2d(kernel_size=2)
        self.dropout = nn.Dropout2d(p=0.2)
        self.relu = nn.ReLU()

    def forward(self, data_inputs):
        x = self.adapt_pool(data_inputs)
        x = self.first_layer(x)
        x = self.max_pool(x)
        x = self.relu(x)

        x = self.second_layer(x)
        x = self.max_pool(x)
        x = self.relu(x)
        x = torch.flatten(x, 1)

        logits = self.fc_layer(x)
        probs = self.classification_layer(logits)
        return logits, probs


class EfficientNetB4(nn.Module):
    def __init__(self, num_channels, num_classes):
        """We need to adjust the input size expected for the EfficientNetB4 Model based on the image
        shapes.
        """
        super().__init__()
        self.efficientnet = torch.hub.load(
            "NVIDIA/DeepLearningExamples:torchhub", "nvidia_efficientnet_widese_b4", pretrained=True
        )
        self.adapt_pool = nn.AdaptiveAvgPool2d((28, 28))
        fc_features = self.efficientnet.classifier.fc.in_features
        self.efficientnet.classifier.fc = nn.Linear(fc_features, num_classes)
        self.efficientnet.stem.conv = nn.Conv2d(
            num_channels, 48, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
        )
        self.classification_layer = nn.Softmax(dim=1)

    def forward(self, data_inputs):
        x = self.adapt_pool(data_inputs)
        logits = self.efficientnet(x)
        probs = self.classification_layer(logits)
        return logits, probs


class DenseNet161(nn.Module):
    def __init__(self, num_channels, num_classes):
        """We need to adjust the input size expected for the DenseNet161 Model based on the image
        shapes.
        """
        super().__init__()
        self.densenet = torch.hub.load("pytorch/vision:v0.10.0", "densenet161", pretrained=True)
        self.adapt_pool = nn.AdaptiveAvgPool2d((28, 28))
        fc_features = self.densenet.classifier.in_features
        self.densenet.classifier = nn.Linear(fc_features, num_classes)
        self.densenet.features.conv0 = nn.Conv2d(
            num_channels, 96, kernel_size=(1, 1), stride=(1, 1), padding=(1, 1), bias=False
        )
        self.classification_layer = nn.Softmax(dim=1)

    def forward(self, data_inputs):
        x = self.adapt_pool(data_inputs)
        logits = self.densenet(x)
        probs = self.classification_layer(logits)
        return logits, probs
