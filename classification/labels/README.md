This directory contains all the data label definitions used for classification.
The label metadata is specified in the [`labels.yaml`](labels.yaml) file. Please
see that file's doc comment for more details.

Additionally, the classification rule Rego code for each label must be specified
as a `<label>.rego` file, where `<label>` is the name of the label in lowercase.
For example, if the label `ADDRESS` is defined in `labels.yaml`, it should have
an `address.rego` file defined as well.
