import argparse

import unipipe
from unipipe import dsl

"""
An example of using GPUs to accelerate pipeline components.  To do that, we
include the 'hardware' keyword in our decorator, and specify what type/count of
accelerator we need.

NOTE: Currently, the accelerator 'type' has no effect for 'python' and 'docker'
executors. In the future, you may be able to specify.

In addition to accelerators, 'Hardware' has the follow inputs/properties:

    Hardware
        cpus (str | int | None) - Requested vCPU cores. Append "m" for "milli-CPUs".
        memory (str | int | None) - Requested RAM. Optionally append "K" (kilobytes),
            "M" (megabytes), or "G" (gigabytes).
        accelerator (Dict | None)
            count (str | int | None) - Number of accelerators
            type (str | None) - Name/type of the accelerator
"""


@dsl.component(
    packages_to_install=["requests", "torch", "torchvision"],
    hardware=dsl.Hardware(
        cpus=4,
        memory="16G",
        accelerator=dsl.Accelerator(
            count=1,
            type=dsl.AcceleratorType.T4,
        ),
    ),
    # Equivalently, hardware can be a dictionary.  Example:
    #   hardware={
    #       "cpus": 4,
    #       "memory": "16G",
    #       "accelerator": {"count": 1, type="nvidia-tesla-t4"},
    #   }
)
def image_classification(image_url: str) -> int:
    import io
    import logging

    import requests
    import torch
    from PIL import Image
    from torchvision.models import efficientnet_b0
    from torchvision.transforms.functional import to_tensor

    # Get the CUDA device, if available
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    logging.info(f"Using device: {device}")

    # Download the image, convert to a Tensor, add 'batch' dimension, and push to GPU.
    image_bytes = requests.get(image_url).content
    image = Image.open(io.BytesIO(image_bytes))
    tensor = to_tensor(image).unsqueeze(0).to(device)

    # Load the classification model, set to 'eval' mode, and push to the GPU.
    model = efficientnet_b0(pretrained=True)
    model.eval().to(device)

    # Classify the image, and return the predicted image class (integer).
    with torch.no_grad():
        logits = model(tensor)
        prediction = logits.argmax(dim=-1).item()

    return prediction


@dsl.pipeline
def pipeline(image_url: str) -> dsl.Component[int]:
    return image_classification(image_url=image_url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-url", required=True)
    parser.add_argument("--executor", default="python")
    args = parser.parse_args()

    unipipe.run(
        executor=args.executor,
        # Pass arguments into the decorated pipeline, as with any Python function.
        pipeline=pipeline(image_url=args.image_url),
    )

    # Tested using this example image of a goldfish:
    #   https://raw.githubusercontent.com/EliSchwartz/imagenet-sample-images/master/n01443537_goldfish.JPEG
    #
    # Expected output:
    #
    # INFO:root:CUDA available: True
    # INFO:root:[image_classification-31097394] - 1
    #
    # A couple of notes:
    #   - It correctly identifies the "goldfish" class (index 1 from ImageNet).
    #   - You may see warning messages from 'torchvision' about the pretrained
    #     model weights, etc. They're unrelated to our pipeline, so just ignore them.
