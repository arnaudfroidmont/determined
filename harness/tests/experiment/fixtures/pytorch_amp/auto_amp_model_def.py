"""
This example demonstrates how to modify a model to use PyTorch's native AMP
(automatic mixed precision) support in Determined.

In the `__init__` method, amp_init() is called (and this accepts parameters to
tune the GradScaler).

The methods `train_batch` and `evaluate_batch` are modified to use an autocast
context during the forward pass.
"""

import typing

import train

from determined import pytorch


class MNistAutoAMPTrial(train.MNistTrial):
    def __init__(
        self, context: pytorch.PyTorchTrialContext, hparams: typing.Optional[typing.Dict]
    ) -> None:
        context.experimental.use_amp()
        super().__init__(context=context, hparams=hparams)
